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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.injenia.dataflow.model.DbRow;
import it.injenia.dataflow.model.MsSqlColumn;
import it.injenia.dataflow.model.MsSqlTable;
import it.injenia.dataflow.util.MsSqlManager;

/**
 * This function queries a SQL table (input) and creates a {@link DbRow} for
 * each row. It returns a key-value structure with the table as key and the a
 * row as value (output).
 * 
 * @author Fausto Fusaro
 *
 */
public class TableToDbRowFn extends DoFn<MsSqlTable, KV<MsSqlTable, DbRow>> {
	private static final long serialVersionUID = -7112778204662444632L;

	private static final Logger _log = LoggerFactory.getLogger(TableToDbRowFn.class);

	private final String driver;
	private final String connectionString;
	private final String username;
	private final String password;
	private final PCollectionView<Map<MsSqlTable, List<MsSqlColumn>>> columnsMap;


	private TableToDbRowFn(Builder reader) {
		this.driver = reader.driver;
		this.connectionString = reader.connectionString;
		this.username = reader.username;
		this.password = reader.password;
		this.columnsMap = reader.columnsMap;
	}
	/**
	 * This internal static class is helpful for constructing the function from a set of input parameters.
	 * 
	 */
	public static class Builder {
		private final String driver;
		private final String connectionString;
		private String username;
		private String password;
		private PCollectionView<Map<MsSqlTable, List<MsSqlColumn>>> columnsMap;

		/**
		 * Constructor
		 * 
		 * @param driver SQL driver string (it should be a constant for MS SQL Server)
		 * @param connectionString the connection string
		 */
		public Builder(String driver, String connectionString) {
			this.driver = driver;
			this.connectionString = connectionString;
		}

		/**
		 * MS SQL username option
		 * 
		 * @param username a string with MS SQL username 
		 * @return a Builder instance with incremental configuration
		 */
		public Builder withUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * MS SQL password option
		 * 
		 * @param password a string with MS SQL password 
		 * @return a Builder instance with incremental configuration
		 */
		public Builder withPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * MS SQL tables structure
		 * 
		 * @param columnsMap the map as specified in the main class documentation.
		 * @return a Builder instance with incremental configuration
		 */
		public Builder withColumnsMap(PCollectionView<Map<MsSqlTable, List<MsSqlColumn>>> columnsMap) {
			this.columnsMap = columnsMap;
			return this;
		}

		/**
		 * Builder creation. It creates an instance of TableToDbRowFn with specified configurations.
		 * 
		 * @return a full configured instance of {@link TableToDbRowFn}
		 */
		public TableToDbRowFn create() {
			return new TableToDbRowFn(this);
		}
	}

	@ProcessElement
	public void processElement(ProcessContext c) {
		MsSqlTable msSqlTable = c.element();
		Map<MsSqlTable, List<MsSqlColumn>> columnsMapInput = c.sideInput(columnsMap);
		List<MsSqlColumn> columnNames = columnsMapInput.get(msSqlTable);

		if (columnNames == null) {
			_log.warn(String.format("Table %s not found in tablesMap!", msSqlTable.getFullName()));
			return;
		}
		Connection connection = null;
		try {
			connection = MsSqlManager.getConnection(this.connectionString, username, password);
			_log.debug("Connection String: " + this.connectionString);
			_log.debug("Connection Catalog: " + connection.getCatalog());
			_log.debug("Connection Schema: " + connection.getSchema());
			Statement statement = connection.createStatement();
			String query = String.format("SELECT * FROM %s.%s", msSqlTable.getSchema(), msSqlTable.getName());
			_log.info("Executing query: " + query);
			ResultSet rs = statement.executeQuery(query);

			ResultSetMetaData meta = rs.getMetaData();
			int columnCount = meta.getColumnCount();
			while (rs.next()) {
				List<Object> values = new ArrayList<Object>();
				for (int colNumber = 1; colNumber <= columnCount; ++colNumber) {
					MsSqlColumn column = columnNames.get(colNumber - 1);
					String columnName = column.getName();
					Object value = rs.getObject(columnName);
					values.add(value);
				}

				DbRow row = DbRow.create(values);
				KV<MsSqlTable, DbRow> kv = KV.of(msSqlTable, row);
				c.output(kv);
			}
			connection.close();

		} catch (SQLException e) {
			_log.warn("Processing error", e);
			try {
				connection.close();
			} catch (SQLException e1) {
			}
		}
	}
}
