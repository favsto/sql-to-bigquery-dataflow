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

package it.injenia.dataflow.transform;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableSchema;

import it.injenia.dataflow.model.DbRow;
import it.injenia.dataflow.model.MsSqlColumn;
import it.injenia.dataflow.model.MsSqlTable;
import it.injenia.dataflow.util.BigQueryUtils;

/**
 * This class decides on which BigQuery table will be written each row. It needs
 * a key-value structure as input. The object MsSqlTable is useful because it
 * contains schema and table name. It requires a map that, for each table,
 * specifies the table structure (list of columns specification) in order to
 * generate BigQuery TableSchema if necessary.
 * 
 * @author Fausto Fusaro
 *
 */
public class BigQueryTableDestination extends DynamicDestinations<KV<MsSqlTable, DbRow>, String> {

	private static final long serialVersionUID = -2929752032929427146L;

	private static final Logger _log = LoggerFactory.getLogger(BigQueryTableDestination.class);

	private Map<MsSqlTable, List<MsSqlColumn>> columnsMap;
	private String datasetName;

	/**
	 * Constructor. It retrieves the map of tables specification and the output
	 * dataset.
	 * 
	 * @param columnsMap
	 *            a map with the list of columns specification for each table
	 * @param datasetName
	 *            a string with the output dataset name
	 */
	public BigQueryTableDestination(Map<MsSqlTable, List<MsSqlColumn>> columnsMap, String datasetName) {
		this.columnsMap = columnsMap;
		this.datasetName = datasetName;
	}

	@Override
	public String getDestination(ValueInSingleWindow<KV<MsSqlTable, DbRow>> element) {
		KV<MsSqlTable, DbRow> kv = element.getValue();
		MsSqlTable table = kv.getKey();
		return datasetName + "." + table.getFullName();
	}

	@Override
	public TableDestination getTable(String tableDefinition) {
		return new TableDestination(tableDefinition, "Table created by a Dataflow pipeline");
	}

	@Override
	public TableSchema getSchema(String tableDefinition) {
		final String tableFullName = tableDefinition.substring(tableDefinition.indexOf('.') + 1,
				tableDefinition.length());
		_log.info("tableDefinition - table full name: " + tableFullName);

		List<MsSqlColumn> tableColumns = null;
		for (MsSqlTable table : columnsMap.keySet()) {
			if (table.getFullName().equals(tableFullName)) {
				tableColumns = columnsMap.get(table);
				break;
			}
		}

		TableSchema tableSchema = null;
		try {
			tableSchema = BigQueryUtils.getBigQuerySchema(tableColumns);
		} catch (SQLException e) {
			_log.error("Unable to translate column list to TableSchema, tableDefinition: " + tableDefinition, e);
		}
		_log.info("tableDefinition - is table schema null? " + (tableSchema == null));
		return tableSchema;
	}

}
