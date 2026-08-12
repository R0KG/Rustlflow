use std::io::Cursor;

use uuid::Uuid;

use crate::storage::dataset_partition_key;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetPartitionFile {
    pub partition_number: i32,
    pub storage_key: String,
    pub bytes: Vec<u8>,
    pub row_count: i64,
}

pub async fn split_csv(
    dataset_id: Uuid,
    source_bytes: Vec<u8>,
    rows_per_partition: usize,
) -> Result<Vec<DatasetPartitionFile>, String> {
    if rows_per_partition == 0 {
        return Err("rows_per_partition must be greater than zero".to_string());
    }

    tokio::task::spawn_blocking(move || {
        split_csv_blocking(dataset_id, source_bytes, rows_per_partition)
    })
    .await
    .map_err(|error| format!("CSV partition task failed: {error}"))?
}

fn split_csv_blocking(
    dataset_id: Uuid,
    source_bytes: Vec<u8>,
    rows_per_partition: usize,
) -> Result<Vec<DatasetPartitionFile>, String> {
    let mut reader = csv::Reader::from_reader(Cursor::new(source_bytes));
    let headers = reader
        .headers()
        .map_err(|error| format!("Could not read CSV headers: {error}"))?
        .clone();

    let mut partitions = Vec::new();
    let mut current_writer: Option<csv::Writer<Vec<u8>>> = None;
    let mut current_row_count = 0_usize;
    let mut partition_number = 0_i32;

    for record in reader.records() {
        let record = record.map_err(|error| format!("Could not read CSV record: {error}"))?;

        if current_writer.is_none() || current_row_count == rows_per_partition {
            if let Some(writer) = current_writer.take() {
                partitions.push(finish_partition(
                    dataset_id,
                    partition_number - 1,
                    writer,
                    current_row_count,
                )?);
            }

            let mut writer = csv::Writer::from_writer(Vec::new());
            writer
                .write_record(&headers)
                .map_err(|error| format!("Could not write CSV headers: {error}"))?;
            current_writer = Some(writer);
            current_row_count = 0;
            partition_number += 1;
        }

        current_writer
            .as_mut()
            .expect("CSV partition writer exists")
            .write_record(&record)
            .map_err(|error| format!("Could not write CSV record: {error}"))?;
        current_row_count += 1;
    }

    if let Some(writer) = current_writer {
        partitions.push(finish_partition(
            dataset_id,
            partition_number - 1,
            writer,
            current_row_count,
        )?);
    }

    Ok(partitions)
}

fn finish_partition(
    dataset_id: Uuid,
    partition_number: i32,
    writer: csv::Writer<Vec<u8>>,
    row_count: usize,
) -> Result<DatasetPartitionFile, String> {
    let bytes = writer
        .into_inner()
        .map_err(|error| format!("Could not finish CSV partition: {error}"))?;
    Ok(DatasetPartitionFile {
        partition_number,
        storage_key: dataset_partition_key(dataset_id, partition_number),
        bytes,
        row_count: row_count as i64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn split_csv_keeps_headers_and_row_boundaries() {
        let dataset_id = Uuid::new_v4();
        let partitions = split_csv(
            dataset_id,
            b"id,name\n1,Alice\n2,Bob\n3,Carol\n4,Dan\n5,Eve\n".to_vec(),
            2,
        )
        .await
        .expect("could not partition CSV");

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].row_count, 2);
        assert_eq!(partitions[1].row_count, 2);
        assert_eq!(partitions[2].row_count, 1);
        assert_eq!(
            partitions[0].storage_key,
            dataset_partition_key(dataset_id, 0)
        );
        assert_eq!(partitions[0].bytes, b"id,name\n1,Alice\n2,Bob\n");
    }
}
