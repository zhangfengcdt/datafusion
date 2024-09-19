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
use datafusion_expr::{ColumnarValue};
use std::sync::Arc;
use arrow::array::Array;
use arrow_schema::Fields;
use datafusion_common::ScalarValue;

// Create a local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    // Define x, y, z, m coordinate values (z and m are optional)
    let x_values = vec![Some(2.1), Some(3.1), Some(4.1), Some(5.1)];
    let y_values = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
    let z_values = vec![Some(1.5), Some(2.5),  Some(2.5),  Some(2.5)];
    let m_values = vec![None, None, None, None];

    // Define the field types for the struct
    let struct_fields = vec![
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
        Field::new("z", DataType::Float64, true),
        Field::new("m", DataType::Float64, true),
    ];

    // Create a StructBuilder with x, y, z, m fields
    let mut struct_builder = StructBuilder::new(
        struct_fields,
        vec![
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
        ],
    );

    // Add values to the StructBuilder
    for ((x, y), (z, m)) in x_values.iter().zip(y_values.iter()).zip(z_values.iter().zip(m_values.iter())) {
        struct_builder.field_builder::<Float64Builder>(0).unwrap().append_option(*x);
        struct_builder.field_builder::<Float64Builder>(1).unwrap().append_option(*y);
        struct_builder.field_builder::<Float64Builder>(2).unwrap().append_option(*z);
        struct_builder.field_builder::<Float64Builder>(3).unwrap().append_option(*m);
        struct_builder.append(true); // Mark the struct entry as valid
    }

    // Build the StructArray
    let struct_array = struct_builder.finish();

    // Create a RecordBatch with the StructArray
    let batch = RecordBatch::try_from_iter(vec![("coordinates", Arc::new(struct_array) as ArrayRef)])?;

    // Create a session context and register the batch
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

// UDF that takes a struct and an offset, moves the point by the offset
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // Define the UDF logic
    let st_move_udf = Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        assert_eq!(args.len(), 2);

        let offset = match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => *v, // Dereference the value
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                return Err(datafusion::error::DataFusionError::Execution("Offset is None".to_string()))
            },
            _ => {
                return Err(datafusion::error::DataFusionError::Execution("Expected a Float64 scalar for the offset".to_string()))
            }
        };

        // Handle only the case where the input is an array (not a scalar)
        match &args[0] {
            ColumnarValue::Array(array) => {
                let struct_array = as_struct_array(array)?;

                let x_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                let y_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
                let z_array = struct_array.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                let m_array = struct_array.column(3).as_any().downcast_ref::<Float64Array>().unwrap();

                assert_eq!(x_array.len(), y_array.len());
                assert_eq!(x_array.len(), z_array.len());

                // Apply the offset to x, y, z, and m coordinates
                let x_builder = Float64Builder::new();
                let y_builder = Float64Builder::new();
                let z_builder = Float64Builder::new();
                let m_builder = Float64Builder::new();

                // Initialize the field definitions (schema) for x, y, and z
                let struct_fields = vec![
                    Field::new("x", DataType::Float64, true),
                    Field::new("y", DataType::Float64, true),
                    Field::new("z", DataType::Float64, true),
                    Field::new("m", DataType::Float64, true),
                ];

                // Combine the field definitions and builders into a StructBuilder
                let mut struct_builder = StructBuilder::new(
                    struct_fields,
                    vec![
                        Box::new(x_builder) as Box<dyn arrow::array::ArrayBuilder>,
                        Box::new(y_builder) as Box<dyn arrow::array::ArrayBuilder>,
                        Box::new(z_builder) as Box<dyn arrow::array::ArrayBuilder>,
                        Box::new(m_builder) as Box<dyn arrow::array::ArrayBuilder>,
                    ],
                );

                for i in 0..x_array.len() {
                    // Append values to x
                    if x_array.is_valid(i) {
                        struct_builder.field_builder::<Float64Builder>(0).unwrap().append_value(x_array.value(i) + offset);
                    } else {
                        struct_builder.field_builder::<Float64Builder>(0).unwrap().append_null();
                    }

                    // Append values to y
                    if y_array.is_valid(i) {
                        struct_builder.field_builder::<Float64Builder>(1).unwrap().append_value(y_array.value(i) + offset);
                    } else {
                        struct_builder.field_builder::<Float64Builder>(1).unwrap().append_null();
                    }

                    // Append values to z
                    if z_array.is_valid(i) {
                        struct_builder.field_builder::<Float64Builder>(2).unwrap().append_value(z_array.value(i) + offset);
                    } else {
                        struct_builder.field_builder::<Float64Builder>(2).unwrap().append_null();
                    }

                    // Append values to z
                    if m_array.is_valid(i) {
                        struct_builder.field_builder::<Float64Builder>(3).unwrap().append_value(m_array.value(i) + offset);
                    } else {
                        struct_builder.field_builder::<Float64Builder>(3).unwrap().append_null();
                    }

                    // After appending all fields for the current row, mark the struct entry as valid
                    struct_builder.append(true);  // Mark this row as valid
                }

                let result_struct = struct_builder.finish();

                Ok(ColumnarValue::Array(Arc::new(result_struct) as ArrayRef))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Expected an array input, but got a scalar.".to_string(),
            )),
        }
    });

    let st_move_udf = create_udf(
        "st_move",
        vec![DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
            Field::new("z", DataType::Float64, true),
            Field::new("m", DataType::Float64, true),
        ])), DataType::Float64],
        Arc::new(DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
            Field::new("z", DataType::Float64, true),
            Field::new("m", DataType::Float64, true),
        ]))),
        Volatility::Immutable,
        st_move_udf,
    );

    ctx.register_udf(st_move_udf.clone());

    // Use the UDF in a query
    let df = ctx.table("t").await?;
    let expr = st_move_udf.call(vec![col("coordinates"), lit(2.0)]);

    let df2 = df.clone().select(vec![expr])?;

    df.clone().show().await?;

    df2.show().await?;

    Ok(())
}
