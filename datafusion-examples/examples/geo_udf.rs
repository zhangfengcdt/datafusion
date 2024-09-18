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
use arrow_schema::Fields;

// // Define a struct for the inputs
// #[derive(Debug, Clone)]
// struct PowerStruct {
//     base: f64,
//     exponent: f64,
// }

// Create a local execution context with an in-memory table
fn create_context() -> Result<SessionContext> {
    // Define the base and exponent data
    let base_values = vec![Some(2.1), Some(3.1), Some(4.1), Some(5.1)];
    let exponent_values = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];

    // Define the field types for the struct
    let struct_fields = vec![
        Field::new("base", DataType::Float64, true),
        Field::new("exponent", DataType::Float64, true),
    ];

    // Create a StructBuilder with two fields (base and exponent)
    let mut struct_builder = StructBuilder::new(
        struct_fields,
        vec![
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
            Box::new(Float64Builder::new()) as Box<dyn arrow::array::ArrayBuilder>,
        ],
    );

    // Add values to the StructBuilder
    for (base, exponent) in base_values.iter().zip(exponent_values.iter()) {
        struct_builder.field_builder::<Float64Builder>(0).unwrap().append_option(*base);
        struct_builder.field_builder::<Float64Builder>(1).unwrap().append_option(*exponent);
        struct_builder.append(true); // Mark the struct entry as valid
    }

    // Build the StructArray
    let struct_array = struct_builder.finish();

    // Create a RecordBatch with the StructArray
    let batch = RecordBatch::try_from_iter(vec![("values", Arc::new(struct_array) as ArrayRef)])?;

    // Create a session context and register the batch
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    Ok(ctx)
}

// Example of a UDF that takes a struct instead of two floats
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // Define the UDF logic
    let power_struct_udf = Arc::new(|args: &[ColumnarValue]| -> Result<ColumnarValue> {
        assert_eq!(args.len(), 1);

        // Handle only the case where the input is an array (not a scalar)
        match &args[0] {
            ColumnarValue::Array(array) => {
                let struct_array = as_struct_array(array)?;

                let base_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                let exponent_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

                assert_eq!(base_array.len(), exponent_array.len());

                let result_array: Float64Array = base_array
                    .iter()
                    .zip(exponent_array.iter())
                    .map(|(base, exponent)| {
                        match (base, exponent) {
                            (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                            _ => None, // Return None if either base or exponent is null
                        }
                    })
                    .collect();

                Ok(ColumnarValue::Array(Arc::new(result_array) as ArrayRef))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Expected an array input, but got a scalar.".to_string(),
            )),
        }
    });

    let power_struct_udf = create_udf(
        "power_struct",
        vec![DataType::Struct(Fields::from(vec![
            Field::new("base", DataType::Float64, true),
            Field::new("exponent", DataType::Float64, true),
        ]))],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        power_struct_udf,
    );

    ctx.register_udf(power_struct_udf.clone());

    // Use the UDF in a query
    let df = ctx.table("t").await?;
    let expr = power_struct_udf.call(vec![col("values")]);

    let df = df.select(vec![expr])?;

    df.show().await?;

    Ok(())
}
