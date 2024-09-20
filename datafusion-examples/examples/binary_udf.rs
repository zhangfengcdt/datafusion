use arrow::array::{Array, ArrayRef, BinaryArray, BinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::physical_plan::functions::{make_scalar_function};
use datafusion::prelude::*;
use std::sync::Arc;
use datafusion::datasource::MemTable;
use datafusion_expr::Volatility;

// Define the UDF that operates on a BinaryArray
fn binary_to_hex(args: &[ArrayRef]) -> Result<ArrayRef> {
    let binary_array = args[0].as_any().downcast_ref::<BinaryArray>().unwrap();

    let hex_strings: Vec<String> = (0..binary_array.len())
        .map(|i| {
            let bytes = binary_array.value(i);
            // Convert binary data to hex string
            hex::encode(bytes)
        })
        .collect();

    // Create a new StringArray from the hex strings
    let result = StringArray::from(hex_strings);
    Ok(Arc::new(result) as ArrayRef)
}

// Register the UDF with DataFusion
fn register_binary_to_hex_udf(ctx: &mut SessionContext) {
    let scalar_fn = make_scalar_function(binary_to_hex);

    // Register the UDF with DataFusion
    let udf = create_udf(
        "binary_to_hex",           // Function name
        vec![DataType::Binary],    // Input type: BinaryArray
        Arc::new(DataType::Utf8),  // Output type: StringArray
        Volatility::Immutable,     // Volatility: Immutable since the function is deterministic
        scalar_fn,
    );

    ctx.register_udf(udf);
}

// Create an in-memory table with binary data
fn create_in_memory_table() -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("binary_column", DataType::Binary, false),
    ]));

    let binary_data = vec![
        vec![0x48, 0x65, 0x6c, 0x6c, 0x6f],  // "Hello"
        vec![0x57, 0x6f, 0x72, 0x6c, 0x64],  // "World"
        vec![0x21],                          // "!"
    ];

    // Use BinaryBuilder to create the BinaryArray from Vec<Vec<u8>>
    let mut builder = BinaryBuilder::new();
    for binary in &binary_data {
        builder.append_value(binary);
    }
    let binary_array = builder.finish();

    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(binary_array) as ArrayRef])?;

    // Create a MemTable from the RecordBatch
    let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;
    Ok(mem_table)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new session context
    let mut ctx = SessionContext::new();

    // Register the UDF
    register_binary_to_hex_udf(&mut ctx);


    // Create an in-memory table
    let mem_table = create_in_memory_table()?;

    // Register the in-memory table with the context
    ctx.register_table("my_table", Arc::new(mem_table))?;

    // Example SQL query using the UDF
    let df = ctx.sql("SELECT binary_to_hex(binary_column) FROM my_table").await?;

    // Show the results
    df.show().await?;

    Ok(())
}
