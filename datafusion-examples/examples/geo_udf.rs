use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, Float64Builder, StructBuilder},
        datatypes::{DataType, Field},
        record_batch::RecordBatch,
    },
    logical_expr::Volatility,
};

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::cast::as_struct_array;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;
use arrow::array::{Array, ArrayBuilder, DictionaryArray, GenericListBuilder, ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::Int32Type;
use arrow_schema::Fields;
use geo_types::Geometry;
use wkt::TryFromWkt;
use datafusion_common::{DataFusionError, ScalarValue};

pub const GEOMETRY_TYPE: &str = "type";
pub const GEOMETRY_TYPE_POINT: &str = "point";
pub const GEOMETRY_TYPE_MULTIPOINT: &str = "multipoint";
pub const GEOMETRY_TYPE_LINESTRING: &str = "linestring";
pub const GEOMETRY_TYPE_MULTILINESTRING: &str = "multilinestring";
pub const GEOMETRY_TYPE_POLYGON: &str = "polygon";
pub const GEOMETRY_TYPE_MULTIPOLYGON: &str = "multipolygon";

// Create a local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    Ok(ctx)
}

fn create_st_geomfromwkt() -> Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> {
    Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
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
    })
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

#[tokio::main]
async fn main() -> Result<()> {
    // Create a context with a table
    let ctx = create_context()?;

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

    // Test the move UDF
    let df = ctx.table("t").await?;
    let expr = st_geomfromwkt_udf.call(vec![lit("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")]);

    let df2 = df.clone().select(vec![expr])?;

    df.clone().show().await?;

    df2.show().await?;

    Ok(())
}
