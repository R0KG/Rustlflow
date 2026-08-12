use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupedCountQuery {
    pub group_column: String,
    pub count_alias: String,
}

pub fn parse_grouped_count(sql: &str) -> Result<GroupedCountQuery, String> {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let select = normalized
        .strip_prefix("select ")
        .ok_or_else(|| "Distributed jobs must start with SELECT".to_string())?;
    let (select_fields, from) = select
        .split_once(" from events group by ")
        .ok_or_else(|| "Distributed jobs require FROM events GROUP BY".to_string())?;
    let fields: Vec<_> = select_fields.split(',').map(str::trim).collect();
    if fields.len() != 2 {
        return Err("Distributed jobs require one group column and COUNT(*)".to_string());
    }
    let group_column = fields[0];
    let count_alias = fields[1]
        .strip_prefix("count(*) as ")
        .ok_or_else(|| "Distributed jobs require COUNT(*) AS an_alias".to_string())?;
    if from.trim() != group_column || !is_identifier(group_column) || !is_identifier(count_alias) {
        return Err("Distributed group and count names must be simple identifiers".to_string());
    }
    Ok(GroupedCountQuery {
        group_column: group_column.to_string(),
        count_alias: count_alias.to_string(),
    })
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub fn merge_count_results(
    query: &GroupedCountQuery,
    results: &[serde_json::Value],
) -> Result<serde_json::Value, String> {
    let mut groups: BTreeMap<String, (serde_json::Value, i64)> = BTreeMap::new();
    for result in results {
        for row in result["rows"]
            .as_array()
            .ok_or_else(|| "Task result has no rows array".to_string())?
        {
            let group = row
                .get(&query.group_column)
                .ok_or_else(|| "Task result lacks group column".to_string())?
                .clone();
            let count = row
                .get(&query.count_alias)
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "Task result has an invalid count".to_string())?;
            let key = serde_json::to_string(&group)
                .map_err(|_| "Could not key group result".to_string())?;
            groups
                .entry(key)
                .and_modify(|(_, total)| *total += count)
                .or_insert((group, count));
        }
    }
    let rows = groups.into_values().map(|(group, count)| {
        serde_json::json!({query.group_column.clone(): group, query.count_alias.clone(): count})
    }).collect::<Vec<_>>();
    Ok(serde_json::json!({"row_count": rows.len(), "rows": rows}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_count_results_sums_the_same_group_across_partitions() {
        let query = parse_grouped_count(
            "SELECT endpoint, COUNT(*) AS requests FROM events GROUP BY endpoint",
        )
        .expect("could not parse grouped count query");
        let result = merge_count_results(
            &query,
            &[
                serde_json::json!({"rows":[{"endpoint":"/payments","requests":3},{"endpoint":"/login","requests":1}]}),
                serde_json::json!({"rows":[{"endpoint":"/payments","requests":2},{"endpoint":"/login","requests":4}]}),
            ],
        )
        .expect("could not merge task results");

        assert_eq!(result["row_count"], 2);
        assert_eq!(
            result["rows"],
            serde_json::json!([
                {"endpoint":"/login","requests":5},
                {"endpoint":"/payments","requests":5}
            ])
        );
    }
}
