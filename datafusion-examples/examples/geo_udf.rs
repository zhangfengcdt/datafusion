use datafusion::{
    arrow::{
        array::{Float64Builder, StructBuilder},
        datatypes::{DataType, Field}
        ,
    },
    logical_expr::Volatility,
};

use arrow::array::{Array, ArrayBuilder, BinaryArray, BooleanBuilder, DictionaryArray, FixedSizeBinaryArray, GenericListBuilder, LargeBinaryArray, ListArray, ListBuilder, StringArray, StringBuilder, UInt8Array};
use arrow::datatypes::Int32Type;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use geo::Intersects;
use geo_types::Geometry;
use std::sync::Arc;
use wkt::TryFromWkt;

pub const GEOMETRY_TYPE: &str = "type";
pub const GEOMETRY_TYPE_POINT: &str = "point";
pub const GEOMETRY_TYPE_MULTIPOINT: &str = "multipoint";
pub const GEOMETRY_TYPE_LINESTRING: &str = "linestring";
pub const GEOMETRY_TYPE_MULTILINESTRING: &str = "multilinestring";
pub const GEOMETRY_TYPE_POLYGON: &str = "polygon";
pub const GEOMETRY_TYPE_MULTIPOLYGON: &str = "multipolygon";

// Create a local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    let num_cores = num_cpus::get();
    let config = SessionConfig::new().with_target_partitions(num_cores);
    let runtime_config = RuntimeConfig::new();
    let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    Ok(ctx)
}

pub fn get_coordinate_fields() -> Vec<Field> {
    vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, true),
        Field::new("m", DataType::Float64, true),
    ]
}

pub fn get_geometry_fields_polygon(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(
            GEOMETRY_TYPE_POLYGON,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(coordinate_fields.clone().into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        )
    ]
}

fn get_list_of_list_of_points_schema(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, ListBuilder<StructBuilder>> {
    // Create the StructBuilder for the innermost geometry (with x, y, z, m)
    let inner_builder = StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    );

    // Create the ListBuilder for the middle geometry
    let middle_builder = ListBuilder::new(inner_builder);

    // Create the outermost ListBuilder
    let outer_builder = ListBuilder::new(middle_builder);

    outer_builder
}

pub fn create_geometry_builder_polygon() -> StructBuilder {
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let polygon_builder = get_list_of_list_of_points_schema(coordinate_fields.clone());

    let geometry_polygon_builder = StructBuilder::new(
        get_geometry_fields_polygon(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(polygon_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_polygon_builder
}

fn append_coordinates(list_builder: &mut ListBuilder<StructBuilder>, x_coords: Vec<f64>, y_coords: Vec<f64>) {
    for (x, y) in x_coords.iter().zip(y_coords.iter()) {
        list_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
        list_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
        list_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
        list_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
        list_builder.values().append(true);
    }
    list_builder.append(true);
}

pub fn append_polygon(geometry_builder: &mut StructBuilder, rings: Vec<(Vec<f64>, Vec<f64>)>) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POLYGON);
    // append_nulls(geometry_builder, &[1, 2, 3, 4, 6]);
    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(1).unwrap();
    for (x_coords, y_coords) in rings.iter() {
        let ring_builder = list_builder.values();
        append_coordinates(ring_builder, x_coords.clone(), y_coords.clone());
    }
    list_builder.append(true);
    geometry_builder.append(true);
}

pub fn geo_to_arrow(geometries: &[Geometry]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = match geometries[0] {
        Geometry::Point(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Poing".to_string())),
        Geometry::MultiPoint(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPoint".to_string())),
        Geometry::LineString(_) => return Err(DataFusionError::Internal("Unsupported geometry type: LineString".to_string())),
        Geometry::MultiLineString(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiLineString".to_string())),
        Geometry::Polygon(_) => create_geometry_builder_polygon(),
        Geometry::GeometryCollection(_) => return Err(DataFusionError::Internal("Unsupported geometry type: GeometryCollection".to_string())),
        Geometry::Rect(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Rect".to_string())),
        Geometry::Triangle(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Triangle".to_string())),
        Geometry::Line(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Line".to_string())),
        Geometry::MultiPolygon(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPolygon".to_string())),
    };

    for geom in geometries {
        match geom {
            Geometry::Polygon(polygon) => {
                let exterior = &polygon.exterior();
                let mut x_coords = Vec::with_capacity(exterior.0.len());
                let mut y_coords = Vec::with_capacity(exterior.0.len());
                for coord in &exterior.0 {
                    x_coords.push(coord.x);
                    y_coords.push(coord.y);
                }
                append_polygon(&mut geometry_builder, vec![(x_coords, y_coords)]);
            }
            _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
        }
    }
    let geometry_array = geometry_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn geo_to_arrow_scalar(geometry: &Geometry) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = match geometry {
        Geometry::Point(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Poing".to_string())),
        Geometry::MultiPoint(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPoint".to_string())),
        Geometry::LineString(_) => return Err(DataFusionError::Internal("Unsupported geometry type: LineString".to_string())),
        Geometry::MultiLineString(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiLineString".to_string())),
        Geometry::Polygon(_) => create_geometry_builder_polygon(),
        Geometry::GeometryCollection(_) => return Err(DataFusionError::Internal("Unsupported geometry type: GeometryCollection".to_string())),
        Geometry::Rect(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Rect".to_string())),
        Geometry::Triangle(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Triangle".to_string())),
        Geometry::Line(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Line".to_string())),
        Geometry::MultiPolygon(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPolygon".to_string())),
    };

    match geometry {
        Geometry::Polygon(polygon) => {
            let exterior = &polygon.exterior();
            let mut x_coords = Vec::with_capacity(exterior.0.len());
            let mut y_coords = Vec::with_capacity(exterior.0.len());
            for coord in &exterior.0 {
                x_coords.push(coord.x);
                y_coords.push(coord.y);
            }
            append_polygon(&mut geometry_builder, vec![(x_coords, y_coords)]);
        }
        _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
    let geometry_array = geometry_builder.finish();
    Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(geometry_array))))
}


fn create_st_geomfromwkt() -> Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> {
    Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> { st_geomfromwkt_internal(&args) } )
}

fn st_geomfromwkt_internal(args: &&[ColumnarValue]) -> Result<ColumnarValue> {
    // Ensure there is exactly one argument
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "Expected exactly one argument".to_string(),
        ));
    }

    // Extract the WKT strings from the argument
    let wkt_value = &args[0];
    let wkt_strings: Vec<String> = match wkt_value {
        ColumnarValue::Array(array) => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values = dict_array.values().as_any().downcast_ref::<StringArray>().unwrap();
                dict_array.keys().iter().map(|key| {
                    let key = key.unwrap();
                    values.value(key as usize).to_string()
                }).collect()
            } else {
                array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!("Expected string input for WKT, but got {:?}", array.data_type())))?
                    .iter()
                    .map(|wkt| wkt.unwrap().to_string())
                    .collect()
            }
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(value)) => {
                let geom = Geometry::try_from_wkt_str(value)
                    .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e)))?;

                let arrow_scalar = geo_to_arrow_scalar(&geom)
                    .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

                return Ok(arrow_scalar)
            },
            _ => return Err(DataFusionError::Internal(format!("Expected Utf8 scalar input for WKT, but got {:?}", scalar))),
        }
    };

    // Create the GEO geometry objects from the WKT strings
    let geoms: Result<Vec<Geometry>, DataFusionError> = wkt_strings.iter()
        .map(|wkt_str| Geometry::try_from_wkt_str(wkt_str)
            .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e))))
        .collect();

    // Convert the GEO geometry objects back to an Arrow array
    let arrow_array = geo_to_arrow(&geoms?)
        .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

    Ok(arrow_array)
}

fn create_st_intersects() -> Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> {
    Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        // Ensure there are exactly two arguments
        if args.len() != 2 {
            return Err(DataFusionError::Internal(
                "Expected exactly two arguments".to_string(),
            ));
        }

        // Extract the WKB binaries from the argument
        let wkb_value = &args[0];
        let wkb_binaries: Vec<Vec<u8>> = match wkb_value {
            ColumnarValue::Array(array) => {
                if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                    let values = dict_array.values().as_any().downcast_ref::<BinaryArray>().unwrap();
                    dict_array.keys().iter().map(|key| {
                        let key = key.unwrap();
                        values.value(key as usize).to_vec()
                    }).collect()
                } else if let Some(binary_array) = array.as_any().downcast_ref::<BinaryArray>() {
                    binary_array.iter().map(|wkb| wkb.unwrap().to_vec()).collect()
                } else {
                    return Err(DataFusionError::Internal(format!("Expected binary input for WKB, but got {:?}", array.data_type())));
                }
            },
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Binary(Some(value)) => vec![value.clone()],
                _ => return Err(DataFusionError::Internal(format!("Expected binary scalar input for WKB, but got {:?}", scalar))),
            }
        };

        // Create GEO geometry objects from the WKB binaries
        let geoms: Result<Vec<Geometry>, DataFusionError> = wkb_binaries.iter()
            .map(|wkb_bin| {
                read_wkb(wkb_bin)
                    .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKB: {:?}", e)))
            })
            .collect();

        // Call the geometry_to_geos function
        let geos_geom_array1 = geoms?;

        let geom2 = &args[1];

        // Convert the second geometry to a GEO geometry object
        let geos_geom2 = match geom2 {
            ColumnarValue::Array(array) => {
                if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                    let values = dict_array.values().as_any().downcast_ref::<StringArray>().unwrap();
                    let wkt_strings: Vec<String> = dict_array.keys().iter().map(|key| {
                        let key = key.unwrap();
                        values.value(key as usize).to_string()
                    }).collect();

                    let geoms: Result<Vec<Geometry>, DataFusionError> = wkt_strings.iter()
                        .map(|wkt_str| Geometry::try_from_wkt_str(wkt_str)
                            .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e))))
                        .collect();

                    geoms?
                } else {
                    let wkt_strings: Vec<String> = array.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Internal(format!("Expected string input for WKT, but got {:?}", array.data_type())))?
                        .iter()
                        .map(|wkt| wkt.unwrap().to_string())
                        .collect();

                    let geoms: Result<Vec<Geometry>, DataFusionError> = wkt_strings.iter()
                        .map(|wkt_str| Geometry::try_from_wkt_str(wkt_str)
                            .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e))))
                        .collect();

                    geoms?
                }
            },
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Utf8(Some(value)) => {
                    let geom = Geometry::try_from_wkt_str(value)
                        .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e)))?;
                    vec![geom]
                },
                _ => return Err(DataFusionError::Internal(format!("Expected Utf8 scalar input for WKT, but got {:?}", scalar))),
            }
        };

        // Call the intersects function on the geometries from array1 and array2 on each element
        let mut boolean_builder = BooleanBuilder::new();

        for g1 in geos_geom_array1.iter() {
            let intersects = g1.intersects(&geos_geom2[0]);
            boolean_builder.append_value(intersects);
        }

        // Finalize the BooleanArray and return the result
        let boolean_array = boolean_builder.finish();
        Ok(ColumnarValue::Array(Arc::new(boolean_array)))
    })
}


#[tokio::main]
async fn main() -> Result<()> {
    // Create a context with a table
    let ctx = create_context()?;

    // register parquet file with the execution context
    ctx.register_parquet(
        "buildings",
        // &format!("/Users/feng/github/datafusion-comet/spark-warehouse/overture-buildings-large/"),
        // &format!("/Users/feng/github/datafusion-comet/spark-warehouse/postal-codes/"),
        &format!("/Users/feng/github/datafusion-comet/spark-warehouse/osm-nodes-large/"),
        ParquetReadOptions::default(),
    )
        .await?;

    // // create a logical plan from a SQL string and then programmatically add new filters
    // let df = ctx
    //     // Use SQL to read some data from the parquet file
    //     .sql(
    //         "SELECT id, version,level, geometry  FROM alltypes_plain limit 10",
    //     )
    //     .await?;
    //
    // df.clone().schema().fields().iter().for_each(|field| {
    //     println!("{:?}", field)
    // });
    // df.clone().show().await?;

    // Create a GEO-UDF
    let st_geomfromwkt_udf = create_udf(
        "st_geomfromwkt",
        vec![DataType::Utf8],
        DataType::Struct(get_geometry_fields_polygon(get_coordinate_fields()).into()),
        Volatility::Immutable,
        create_st_geomfromwkt()
    );
    // Register the UDF
    ctx.register_udf(st_geomfromwkt_udf.clone());

    // Create a GEO-UDF
    let st_intersects_udf = create_udf(
        "st_intersects",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        create_st_intersects()
    );
    // Register the UDF
    ctx.register_udf(st_intersects_udf.clone());



    let df2 = ctx
        // Use SQL to read some data from the parquet file
        .sql(
            // small range
            "select count(1) from buildings where st_intersects(cast(geometry as BYTEA), 'polygon((-118.58307129967345 34.31439167411405,-118.6132837020172 33.993916507403284,-118.3880639754547 33.708792488814765,-117.64374024498595 33.43188776025067,-117.6135278426422 33.877700857313904,-117.64923340904845 34.19407205090323,-118.14911133873595 34.35748320631873,-118.58307129967345 34.31439167411405))')"
            // medium range
            // "select count(1) from buildings where st_intersects(cast(geometry as BYTEA), 'polygon((-124.4009 41.9983,-123.6237 42.0024,-123.1526 42.0126,-122.0073 42.0075,-121.2369 41.9962,-119.9982 41.9983,-120.0037 39.0021,-117.9575 37.5555,-116.3699 36.3594,-114.6368 35.0075,-114.6382 34.9659,-114.6286 34.9107,-114.6382 34.8758,-114.5970 34.8454,-114.5682 34.7890,-114.4968 34.7269,-114.4501 34.6648,-114.4597 34.6581,-114.4322 34.5869,-114.3787 34.5235,-114.3869 34.4601,-114.3361 34.4500,-114.3031 34.4375,-114.2674 34.4024,-114.1864 34.3559,-114.1383 34.3049,-114.1315 34.2561,-114.1651 34.2595,-114.2249 34.2044,-114.2221 34.1914,-114.2908 34.1720,-114.3237 34.1368,-114.3622 34.1186,-114.4089 34.1118,-114.4363 34.0856,-114.4336 34.0276,-114.4652 34.0117,-114.5119 33.9582,-114.5366 33.9308,-114.5091 33.9058,-114.5256 33.8613,-114.5215 33.8248,-114.5050 33.7597,-114.4940 33.7083,-114.5284 33.6832,-114.5242 33.6363,-114.5393 33.5895,-114.5242 33.5528,-114.5586 33.5311,-114.5778 33.5070,-114.6245 33.4418,-114.6506 33.4142,-114.7055 33.4039,-114.6973 33.3546,-114.7302 33.3041,-114.7206 33.2858,-114.6808 33.2754,-114.6698 33.2582,-114.6904 33.2467,-114.6794 33.1720,-114.7083 33.0904,-114.6918 33.0858,-114.6629 33.0328,-114.6451 33.0501,-114.6286 33.0305,-114.5888 33.0282,-114.5750 33.0351,-114.5174 33.0328,-114.4913 32.9718,-114.4775 32.9764,-114.4844 32.9372,-114.4679 32.8427,-114.5091 32.8161,-114.5311 32.7850,-114.5284 32.7573,-114.5641 32.7503,-114.6162 32.7353,-114.6986 32.7480,-114.7220 32.7191,-115.1944 32.6868,-117.3395 32.5121,-117.4823 32.7838,-117.5977 33.0501,-117.6814 33.2341,-118.0591 33.4578,-118.6290 33.5403,-118.7073 33.7928,-119.3706 33.9582,-120.0050 34.1925,-120.7164 34.2561,-120.9128 34.5360,-120.8427 34.9749,-121.1325 35.2131,-121.3220 35.5255,-121.8013 35.9691,-122.1446 36.2808,-122.1721 36.7268,-122.6871 37.2227,-122.8903 37.7783,-123.2378 37.8965,-123.3202 38.3449,-123.8338 38.7423,-123.9793 38.9946,-124.0329 39.3088,-124.0823 39.7642,-124.5314 40.1663,-124.6509 40.4658,-124.3144 41.0110,-124.3419 41.2386,-124.4545 41.7170,-124.4009 41.9983))')",
            // large range
            // "select count(1) from buildings where st_intersects(cast(geometry as BYTEA), 'polygon ((-179.99989999978519 16.152429930674884, -179.99989999978519 71.86717445333835, -66.01355466931244 71.86717445333835, -66.01355466931244 16.152429930674884, -179.99989999978519 16.152429930674884))')"
        )
        .await?;

    // Start timing
    let start = Instant::now();

    df2.clone().show().await?;

    // End timing
    let duration = start.elapsed();
    println!("Time taken: {:.2?} seconds", duration.as_secs_f64());

    //
    // // Test the move UDF
    // let expr = st_geomfromwkt_udf.call(vec![lit("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")]);
    //
    // let df2 = df.clone().select(vec![expr])?;
    //
    // df.clone().show().await?;
    //
    // df2.show().await?;

    Ok(())
}



// Below is the code from the original datafusion-comet crate

use geo::{
    Coord, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};
use std::io;
use std::io::{Cursor, Read};
use std::time::Instant;
use arrow::ipc::BinaryView;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

/**
 * Read a WKB encoded geometry from a cursor. The cursor will be advanced by the size of the read geometry.
 * This function is safe for malformed WKB. You should specify a max number of field values to prevent
 * memory exhaustion.
 */
pub fn read_wkb_from_cursor_safe<T: AsRef<[u8]>>(
    cursor: &mut Cursor<T>,
    max_num_field_value: i32,
) -> Result<Geometry, Error> {
    let mut reader = WKBReader::wrap(cursor, max_num_field_value);
    reader.read()
}

/**
 * Read a WKB encoded geometry from a cursor. The cursor will be advanced by the size of the read geometry.
 * This function is unsafe for malformed WKB.
 */
pub fn read_wkb_from_cursor<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Geometry, Error> {
    let mut reader = WKBReader::wrap(cursor, i32::MAX);
    reader.read()
}

/**
 * Read WKB from a byte slice.
 * This function is safe for malformed WKB. You should specify a max number of field values to prevent
 * memory exhaustion.
 */
pub fn read_wkb_safe<T: AsRef<[u8]>>(
    bytes: &T,
    max_num_field_value: i32,
) -> Result<Geometry, Error> {
    let mut cursor = Cursor::new(bytes);
    read_wkb_from_cursor_safe(&mut cursor, max_num_field_value)
}

/**
 * Read WKB from a byte slice. This function is unsafe for malformed WKB.
 */
pub fn read_wkb<T: AsRef<[u8]>>(bytes: &T) -> Result<Geometry, Error> {
    let mut cursor = Cursor::new(bytes);
    read_wkb_from_cursor(&mut cursor)
}

/**
 * Reader for reading WKB encoded geometries from a cursor. This reader supports both EWKB and ISO/OGC standards.
 * Due to the limitation of geo, it only supports XY coordinates, and will ignore
 * Z and M values if present.
 */
pub struct WKBReader<'c, T: AsRef<[u8]>> {
    cursor: &'c mut Cursor<T>,
    max_num_field_value: i32,
}

impl<'c, T: AsRef<[u8]>> WKBReader<'c, T> {
    /**
     * Wrap a cursor to read WKB.
     */
    pub fn wrap(cursor: &'c mut Cursor<T>, max_num_field_value: i32) -> Self {
        Self {
            cursor,
            max_num_field_value,
        }
    }

    /**
     * Read the next geometry from the cursor. The wrapped cursor will be advanced by the
     * size of the read geometry.
     */
    pub fn read(&mut self) -> Result<Geometry, Error> {
        self._read(None)
    }

    fn _read(&mut self, expected_type: Option<u32>) -> Result<Geometry, Error> {
        let mut byte_order = [0u8; 1];
        self.cursor
            .read_exact(&mut byte_order)
            .map_err(|e| Error::io_error(e, self.cursor))?;
        match byte_order[0] {
            0 => self.read_geometry::<BigEndian>(expected_type),
            1 => self.read_geometry::<LittleEndian>(expected_type),
            _ => Err(Error::new(ErrorKind::InvalidByteOrder, self.cursor)),
        }
    }

    fn read_geometry<BO: ByteOrder>(
        &mut self,
        expected_type: Option<u32>,
    ) -> Result<Geometry, Error> {
        let mut buf_reader = ByteBufferReader::<BO>::new(self.max_num_field_value);
        let type_code = buf_reader.read_u32(self.cursor)?;

        /*
         * To get geometry type mask out EWKB flag bits,
         * and use only low 3 digits of type word.
         * This supports both EWKB and ISO/OGC.
         */
        let geometry_type = (type_code & 0xffff) % 1000;
        if let Some(expected_type) = expected_type {
            if geometry_type != expected_type {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        let has_z = (type_code & 0x80000000) != 0
            || (type_code & 0xffff) / 1000 == 1
            || (type_code & 0xffff) / 1000 == 3;
        let has_m = (type_code & 0x40000000) != 0
            || (type_code & 0xffff) / 1000 == 2
            || (type_code & 0xffff) / 1000 == 3;

        // determine if SRIDs are present (EWKB only)
        let has_srid = (type_code & 0x20000000) != 0;
        if has_srid {
            let _ = buf_reader.read_i32(self.cursor)?;
        }

        // Read coordinates based on dimension
        let ordinals = if has_z && has_m {
            4 // XYZM
        } else if has_z || has_m {
            3 // XYZ or XYM
        } else {
            2 // XY
        };

        match geometry_type {
            1 => buf_reader.read_point(ordinals, self.cursor),
            2 => buf_reader.read_linestring(ordinals, self.cursor),
            3 => buf_reader.read_polygon(ordinals, self.cursor),
            4 => {
                let num_points = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multipoint(num_points)
            }
            5 => {
                let num_lines = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multilinestring(num_lines)
            }
            6 => {
                let num_polygons = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multipolygon(num_polygons)
            }
            7 => {
                let num_geoms = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_geometrycollection(num_geoms)
            }
            _ => Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor)),
        }
    }

    fn read_multipoint(&mut self, num_points: i32) -> Result<Geometry, Error> {
        let mut points = Vec::with_capacity(num_points as usize);
        for _ in 0..num_points {
            let point = self._read(Some(1))?;
            if let Geometry::Point(p) = point {
                points.push(p);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiPoint(MultiPoint::new(points)))
    }

    fn read_multilinestring(&mut self, num_lines: i32) -> Result<Geometry, Error> {
        let mut lines = Vec::with_capacity(num_lines as usize);
        for _ in 0..num_lines {
            let line = self._read(Some(2))?;
            if let Geometry::LineString(l) = line {
                lines.push(l);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiLineString(MultiLineString::new(lines)))
    }

    fn read_multipolygon(&mut self, num_polygons: i32) -> Result<Geometry, Error> {
        let mut polygons = Vec::with_capacity(num_polygons as usize);
        for _ in 0..num_polygons {
            let polygon = self._read(Some(3))?;
            if let Geometry::Polygon(p) = polygon {
                polygons.push(p);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiPolygon(MultiPolygon::new(polygons)))
    }

    fn read_geometrycollection(&mut self, num_geoms: i32) -> Result<Geometry, Error> {
        let mut geoms = Vec::with_capacity(num_geoms as usize);
        for _ in 0..num_geoms {
            let geom = self._read(None)?;
            geoms.push(geom);
        }
        Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
            geoms,
        )))
    }
}

struct ByteBufferReader<BO: ByteOrder> {
    byte_order: std::marker::PhantomData<BO>,
    i32_upper_bound: i32,
}

impl<BO: ByteOrder> ByteBufferReader<BO> {
    #[inline]
    pub fn new(i32_upper_bound: i32) -> Self {
        Self {
            byte_order: std::marker::PhantomData::<BO>,
            i32_upper_bound,
        }
    }

    #[inline]
    fn read_i32<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<i32, Error> {
        let result = cursor.read_i32::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_i32_with_bound<T: AsRef<[u8]>>(
        &mut self,
        cursor: &mut Cursor<T>,
    ) -> Result<i32, Error> {
        let value = self.read_i32(cursor)?;
        if value < 0 || value > self.i32_upper_bound {
            return Err(Error::new(ErrorKind::InvalidNumberValue, cursor));
        }
        Ok(value)
    }

    #[inline]
    fn read_u32<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<u32, Error> {
        let result = cursor.read_u32::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_f64<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<f64, Error> {
        let result = cursor.read_f64::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_coord<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Coord<f64>, Error> {
        let x = self.read_f64(cursor)?;
        let y = self.read_f64(cursor)?;

        // Read and discard any additional ordinates (Z and/or M)
        for _ in 2..ordinals {
            let _ = self.read_f64(cursor)?;
        }

        Ok(Coord::from((x, y)))
    }

    #[inline]
    fn read_coords<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Vec<Coord<f64>>, Error> {
        let num = self.read_i32_with_bound(cursor)?;
        let mut coords = Vec::with_capacity(num as usize);
        for _ in 0..num {
            coords.push(self.read_coord(ordinals, cursor)?);
        }
        Ok(coords)
    }

    pub fn read_point<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let coord = self.read_coord(ordinals, cursor)?;
        Ok(Geometry::Point(Point(coord)))
    }

    pub fn read_linestring<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let coords = self.read_coords(ordinals, cursor)?;
        Ok(Geometry::LineString(LineString::new(coords)))
    }

    pub fn read_polygon<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let num_rings = self.read_i32_with_bound(cursor)? as usize;
        if num_rings == 0 {
            return Ok(Geometry::Polygon(Polygon::new(
                LineString::new(vec![]),
                vec![],
            )));
        }

        let exterior = LineString::new(self.read_coords(ordinals, cursor)?);
        let mut interiors = Vec::with_capacity(num_rings - 1);
        for _ in 0..(num_rings - 1) {
            let coords = self.read_coords(ordinals, cursor)?;
            interiors.push(LineString::new(coords));
        }
        Ok(Geometry::Polygon(Polygon::new(exterior, interiors)))
    }
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    InvalidByteOrder,
    InvalidGeometryType,
    InvalidCoordinateDimension,
    InvalidNumberValue,
    IOError,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub pos: usize,
    pub outer: Option<io::Error>,
}

impl Error {
    pub fn new<T: AsRef<[u8]>>(kind: ErrorKind, cursor: &Cursor<T>) -> Self {
        Error {
            kind,
            pos: cursor.position() as usize,
            outer: None,
        }
    }

    pub fn io_error<T: AsRef<[u8]>>(e: io::Error, cursor: &Cursor<T>) -> Self {
        Error {
            kind: ErrorKind::IOError,
            pos: cursor.position() as usize,
            outer: Some(e),
        }
    }
}
