use anyhow::Result;
use sqlparser::{
    ast::{self},
    dialect::MySqlDialect,
    parser::Parser,
};

use crate::cleaner::config::Config;

pub struct SqlValidator<'a> {
    config: &'a Config,
}

impl<'a> SqlValidator<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }

    pub fn validate_sql_query(&self, sql: &str) -> Result<(), anyhow::Error> {
        let dialect = MySqlDialect {};
        let ast = Parser::parse_sql(&dialect, sql)
            .map_err(|e| anyhow::anyhow!("Failed to parse SQL: {}", e))?;

        if ast.len() != 1 {
            return Err(anyhow::anyhow!("Only single SQL statement is allowed"));
        }

        let stmt = &ast[0];

        // Check if it's a DELETE statement and extract the WHERE clause
        let (selection, ..) = match stmt {
            sqlparser::ast::Statement::Delete(delete) => (&delete.selection, &delete.from),
            _ => return Err(anyhow::anyhow!("Only DELETE statements are allowed")),
        };
        if selection.is_none() {
            return Err(anyhow::anyhow!(
                "DELETE statement must have a WHERE clause and FROM clause"
            ));
        } else {
            let selection = selection.as_ref().unwrap();
            if !self.contains_date_sub(selection) {
                return Err(anyhow::anyhow!(
                    "DELETE statement must use DATE_SUB function"
                ));
            }
        }

        Ok(())
    }

    fn validate_interval(&self, interval: &ast::Interval) -> bool {
        match (&*interval.value, &interval.leading_field) {
            (
                ast::Expr::Value(ast::Value::Number(value, false)), // false means not negative
                Some(ast::DateTimeField::Month),
            ) => {
                if let Ok(months) = value.parse::<u64>() {
                    months * 30 >= self.config.safe_mode.retention_days
                } else {
                    false
                }
            }
            (
                ast::Expr::Value(ast::Value::Number(value, false)),
                Some(ast::DateTimeField::Year),
            ) => {
                if let Ok(years) = value.parse::<u64>() {
                    years * 365 >= self.config.safe_mode.retention_days
                } else {
                    false
                }
            }
            (ast::Expr::Value(ast::Value::Number(value, false)), Some(ast::DateTimeField::Day)) => {
                if let Ok(days) = value.parse::<u64>() {
                    days >= self.config.safe_mode.retention_days
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn contains_date_sub(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::BinaryOp {
                left, right, op, ..
            } => {
                if op == &ast::BinaryOperator::Lt
                    || op == &ast::BinaryOperator::LtEq
                    || op == &ast::BinaryOperator::And
                {
                    self.contains_date_sub(left) || self.contains_date_sub(right)
                } else {
                    false
                }
            }
            ast::Expr::Function(ast::Function { name, args, .. }) => {
                if name.to_string().to_uppercase() == "DATE_SUB" {
                    // Check function arguments here
                    match args {
                        ast::FunctionArguments::List(arg_list) => arg_list.args.iter().any(|arg| {
                            matches!(
                                arg,
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                    ast::Expr::Interval(interval)
                                )) if self.validate_interval(interval)
                            )
                        }),
                        _ => false,
                    }
                } else {
                    false
                }
            }
            ast::Expr::InSubquery { expr, subquery, .. } => {
                self.contains_date_sub(expr)
                    || match &*subquery.body {
                        ast::SetExpr::Select(select_box) => {
                            if let Some(selection) = &select_box.selection {
                                self.contains_date_sub(selection)
                            } else if self.contains_date_sub_in_from(&select_box.from) {
                                // Check if any table in FROM clause has a subquery that contains
                                // DATE_SUB
                                true
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
            }
            _ => false,
        }
    }

    fn contains_date_sub_in_from(&self, from: &[ast::TableWithJoins]) -> bool {
        from.iter().any(|table_with_joins| {
            if let ast::TableFactor::Derived { subquery, .. } = &table_with_joins.relation {
                match &*subquery.body {
                    ast::SetExpr::Select(nested_select) => {
                        // Check selection of nested select
                        if let Some(nested_selection) = &nested_select.selection {
                            if self.contains_date_sub(nested_selection) {
                                return true;
                            }
                        }

                        // Recursively check FROM clause of nested select
                        if !nested_select.from.is_empty() {
                            return self.contains_date_sub_in_from(&nested_select.from);
                        }

                        false
                    }
                    _ => false,
                }
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cleaner::template::TemplateEngine;

    #[test]
    fn test_sql_query_ast() {
        // Test cases with different template queries
        let test_cases = vec![
            (
                "DELETE FROM {{ table_name }} WHERE created_at < DATE_SUB('{{ data_interval_end }}', INTERVAL 1 MONTH) LIMIT 1;",
                "validation_runs",
                "2024-03-20 00:00:00",
            ),
            (
                "DELETE FROM {{ table_name }} WHERE id IN (SELECT id FROM (SELECT id FROM {{ table_name }} WHERE created_at < DATE_SUB('{{ data_interval_end }}', INTERVAL 30 DAY) AND id NOT IN (SELECT id FROM {{ table_name }} GROUP BY job_id ORDER BY version DESC LIMIT 10)) as t) LIMIT 1;",
                "job_versions", 
                "2024-03-20 00:00:00"
            ),
        ];

        for (template_query, table_name, data_interval_end) in test_cases {
            // Create template engine and render query
            let template_engine = TemplateEngine::new();
            let parameters = std::collections::HashMap::from([(
                "table_name".to_string(),
                table_name.to_string(),
            )]);

            let sql = template_engine
                .render(template_query, &parameters, data_interval_end)
                .unwrap();

            // Parse SQL and print AST
            let dialect = MySqlDialect {};
            let ast = Parser::parse_sql(&dialect, &sql).unwrap();

            println!("Rendered SQL:\n{}\n", sql);
            println!("SQL AST:\n{:#?}", ast);

            // Basic assertions
            assert_eq!(ast.len(), 1, "Should have exactly one statement");
            match &ast[0] {
                sqlparser::ast::Statement::Delete { .. } => (),
                _ => panic!("Expected DELETE statement"),
            }

            let config = Config::default();
            let validator = SqlValidator::new(&config);
            let is_query_valid = validator.validate_sql_query(&sql);
            println!("is_query_valid: {:?}", is_query_valid);
            assert!(is_query_valid.is_ok(), "Query should be valid");
        }
    }
}
