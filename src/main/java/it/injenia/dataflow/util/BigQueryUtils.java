/*
 * Copyright 2017 Fausto Fusaro - Injenia Srl
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.injenia.dataflow.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import it.injenia.dataflow.model.MsSqlColumn;
import it.injenia.dataflow.model.enumtype.MsSqlDataType;

/**
 * This is a utility class for interacting with BigQuery.
 * 
 * @author Fausto Fusaro
 *
 */
public class BigQueryUtils {
	private static final Logger _log = LoggerFactory.getLogger(BigQueryUtils.class);

	/**
	 * Starting from the list of columns this method returns a {@link TableSchema} definition for BigQuery.
	 * 
	 * @param tableColumns a list of columns specification
	 * @returna {@link TableSchema} with the definition of a BigQuery table
	 * @throws SQLException if something goes wrong with SQL Server data types recognition
	 */
	public static TableSchema getBigQuerySchema(List<MsSqlColumn> tableColumns) throws SQLException{
		if (tableColumns == null)
			return null;
		
		TableSchema schema = new TableSchema();
		List<TableFieldSchema> fieldSchemas = new ArrayList<TableFieldSchema>();
		for (MsSqlColumn column : tableColumns) {
			TableFieldSchema fieldSchema = new TableFieldSchema();
			fieldSchema.setName(column.getName());
			String dataTypeString = column.getDataType().toUpperCase();
			MsSqlDataType dataType;
			try {
				dataType = MsSqlDataType.valueOf(dataTypeString);
			} catch (Exception e) {
				_log.error(String.format("Unrecognized data type %s", dataTypeString));
				throw new SQLException(String.format("Unrecognized data type %s", dataTypeString));
			}
			if (PipelineStructures.msSqlToBqTypeMap.containsKey(dataType)){
				fieldSchema.setType(PipelineStructures.msSqlToBqTypeMap.get(dataType));
			} else {
				_log.error(String.format("DataType %s not supported!", dataType));
				throw new SQLException(String.format("DataType %s not supported!", dataType));
			}
			fieldSchemas.add(fieldSchema);
		}
		schema.setFields(fieldSchemas);
		
        return schema;
	}
}
