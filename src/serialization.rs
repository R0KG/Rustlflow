use datafusion::arrow::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray, TimestampSecondArray, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, TimeUnit},
};

pub fn record_batches_to_json(batches: &[RecordBatch]) -> Result<Vec<serde_json::Value>, String> {
    let mut rows = Vec::new();

    for batch in batches {
        let schema = batch.schema();

        for row_index in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();

            for (column_index, field) in schema.fields().iter().enumerate() {
                let array = batch.column(column_index);
                let value = arrow_value_to_json(array.as_ref(), row_index)?;
                row.insert(field.name().to_string(), value);
            }

            rows.push(serde_json::Value::Object(row));
        }
    }

    Ok(rows)
}

fn arrow_value_to_json(array: &dyn Array, row_index: usize) -> Result<serde_json::Value, String> {
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "Could not read UTF-8 column".to_string())?;
            Ok(serde_json::Value::String(
                values.value(row_index).to_string(),
            ))
        }
        DataType::Int32 => {
            let values = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "Could not read Int32 column".to_string())?;
            Ok(serde_json::Value::Number(
                i64::from(values.value(row_index)).into(),
            ))
        }
        DataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "Could not read Int64 column".to_string())?;
            Ok(serde_json::Value::Number(values.value(row_index).into()))
        }
        DataType::UInt32 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "Could not read UInt32 column".to_string())?;
            Ok(serde_json::Value::Number(
                u64::from(values.value(row_index)).into(),
            ))
        }
        DataType::UInt64 => {
            let values = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "Could not read UInt64 column".to_string())?;
            Ok(serde_json::Value::Number(values.value(row_index).into()))
        }
        DataType::Float32 => {
            let values = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "Could not read Float32 column".to_string())?;
            serde_json::Number::from_f64(f64::from(values.value(row_index)))
                .map(serde_json::Value::Number)
                .ok_or_else(|| "Float32 value cannot be represented as JSON".to_string())
        }
        DataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "Could not read Float64 column".to_string())?;
            serde_json::Number::from_f64(values.value(row_index))
                .map(serde_json::Value::Number)
                .ok_or_else(|| "Float64 value cannot be represented as JSON".to_string())
        }
        DataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "Could not read Boolean column".to_string())?;
            Ok(serde_json::Value::Bool(values.value(row_index)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let values = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| "....".to_string())?;
            Ok(serde_json::Value::Number(values.value(row_index).into()))
        }
        data_type => Err(format!("Unsupported Arrow data type: {data_type:?}")),
    }
}
