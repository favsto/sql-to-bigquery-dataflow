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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableSchema;

import it.injenia.dataflow.model.MsSqlColumn;
import it.injenia.dataflow.model.MsSqlTable;
import it.injenia.dataflow.model.enumtype.MsSqlDataType;

/**
 * This is a utility class for interacting with a MS SQL Server instance.
 * 
 * @author Fausto Fusaro
 *
 */
public class MsSqlManager {
	private static final Logger _log = LoggerFactory.getLogger(MsSqlManager.class);
	
	private static final String CONNECTION_STRING_TEMPLATE = "jdbc:sqlserver://%s:%s;DatabaseName=%s";
	private static final String MS_SQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private static final String QUERY_TABLES = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES";
	private static final String QUERY_COLUMNS = "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=? ORDER BY ORDINAL_POSITION ASC";

	/**
	 * Returns MS SQL Server driver string
	 * 
	 * @return MS SQL Server driver string
	 */
	public static String getDriver(){
		return MS_SQL_DRIVER;
	}
	
	/**
	 * Creates a connection starting from specified parameters.
	 * 
	 * @param address SQL Server instance address
	 * @param port SQL Server instance port
	 * @param catalog catalog name
	 * @param username username
	 * @param password password
	 * @return object with an opened connection manager
	 * @throws SQLException if connection fails or the driver is wrong
	 */
	public static Connection getConnection(String address, int port, String catalog, String username, String password) throws SQLException {
		return getConnection(getConnectionString(address, port, catalog), username, password);
	}
	
	/**
	 * Creates a connection starting from specified parameters.
	 * 
	 * @param connectionString a complete MS SQL connection string, with the specification of database name  
	 * @param username username
	 * @param password password
	 * @return object with an opened connection manager
	 * @throws SQLException if connection fails or the driver is wrong
	 */
	public static Connection getConnection(String connectionString, String username, String password) throws SQLException {
		Connection connection = null;
		try {
			Class.forName(getDriver());
			connection = DriverManager.getConnection(connectionString, username, password);
		} catch (SQLException e) {
			_log.error("MS SQL Connection Failed.", e);
			throw e;
		} catch (ClassNotFoundException e) {
			_log.error("MS SQL driver not found", e);
			throw new SQLException(e);
		}
		return connection;
	}
	
	/**
	 * Creates a connection string based on specified parameters.
	 * 
	 * @param address SQL Server instance address
	 * @param port SQL Server instance port
	 * @param catalog catalog name
	 * @return MS SQL connection string
	 */
	public static String getConnectionString(String address, int port, String catalog){
		return String.format(CONNECTION_STRING_TEMPLATE, address, port, catalog);
	}
	
	/**
	 * This method queries the information schema of MS SQL Server instance, filtered with the catalog name, in order to obtain the list of tables.
	 * 
	 * @param address SQL Server instance address
	 * @param port SQL Server instance port
	 * @param catalog catalog name
	 * @param username username
	 * @param password password
	 * @return a {@link List} of objects {@link MsSqlTable} 
	 * @throws SQLException if SQL a error occurs
	 */
	public static List<MsSqlTable> getTablesList(String address, int port, String catalog, String username, String password) throws SQLException{
		Connection connection = null;
		try {
			connection = getConnection(address, port, catalog, username, password);
			List<MsSqlTable> tables = getTablesList(connection);
			connection.close();
			
			return tables;
		} catch (SQLException e) {
			_log.error("MS SQL error", e);
			try {
				connection.close();
			} catch (Exception e1) {}
			throw e;
		}
	}
	
	/**
	 * This method queries the information schema of MS SQL Server instance, filtered with the catalog name, in order to obtain the list of tables.
	 * 
	 * @param connection a connection object with a valid established connection; see{@link getConnection()}
	 * @return a {@link List} of objects {@link MsSqlTable} 
	 * @throws SQLException if SQL a error occurs
	 */
	public static List<MsSqlTable> getTablesList(Connection connection) throws SQLException{
		List<MsSqlTable> tables = new ArrayList<MsSqlTable>();
		try {
			Statement statement = connection.createStatement();
			
			ResultSet rs = statement.executeQuery(QUERY_TABLES);
			while (rs.next()) {
				MsSqlTable table = new MsSqlTable();
				table.setSchema(rs.getString("TABLE_SCHEMA"));
				table.setName(rs.getString("TABLE_NAME"));
				table.setType(rs.getString("TABLE_TYPE"));
				
				// ignoring views
				if (table.getType().equalsIgnoreCase("view"))
					continue;
				
				tables.add(table);
				_log.debug("Read table: " + table);
			}
		} catch (SQLException e) {
			_log.error("MS SQL error", e);
			throw e;
		}
		return tables;
	}
	
	/**
	 * Starting from a connection (established with your parameters) and a table name it retrieves an ordered list of columns. 
	 * 
	 * @param address SQL Server instance address
	 * @param port SQL Server instance port
	 * @param catalog catalog name
	 * @param username username
	 * @param password password
	 * @param tableName the name of the table on MS SQL Server
	 * @return a {@link List} of objects {@link MsSqlColumn} 
	 * @throws SQLException if SQL a error occurs
	 */
	public static List<MsSqlColumn> getColumnsList(String address, int port, String catalog, String username, String password, String tableName) throws SQLException{
		Connection connection = null;
		try {
			connection = getConnection(address, port, catalog, username, password);
			List<MsSqlColumn> columns = getColumnsList(connection, tableName);
			connection.close();
			
			return columns;
		} catch (SQLException e) {
			_log.error("MS SQL error", e);
			try {
				connection.close();
			} catch (Exception e1) {}
			throw e;
		}
	}
	
	/**
	 * Starting from a connection and a table name it retrieves an ordered list of columns.
	 * 
	 * @param connection a connection object with a valid established connection; see{@link getConnection()}
	 * @param tableName the name of the table on MS SQL Server
	 * @return a {@link List} of objects {@link MsSqlColumn} 
	 * @throws SQLException if SQL a error occurs
	 */
	public static List<MsSqlColumn> getColumnsList(Connection connection, String tableName) throws SQLException{
		List<MsSqlColumn> columns = new ArrayList<MsSqlColumn>();
		try {
			PreparedStatement statement = connection.prepareStatement(QUERY_COLUMNS);
			statement.setString(1, tableName);
			
			ResultSet rs = statement.executeQuery();
			while (rs.next()) {
				MsSqlColumn column = new MsSqlColumn();
				column.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
				column.setName(rs.getString("COLUMN_NAME"));
				column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
				column.setNullable(rs.getBoolean("IS_NULLABLE"));
				column.setDataType(rs.getString("DATA_TYPE"));
				columns.add(column);
				
				// this statement tests the presence of the data type 
				try {
					MsSqlDataType.valueOf(column.getDataType().toUpperCase());
				} catch (Exception e) {
					_log.error(String.format("Unrecognized data type %s in table %s", column.getDataType(), tableName));
				}
			}
		} catch (SQLException e) {
			_log.error("MS SQL error", e);
			throw e;
		}
		return columns;
	}
	
	public static TableSchema getBigQuerySchema(String address, int port, String catalog, String username, String password, String tableName) throws SQLException{
		List<MsSqlColumn> tableColumns = getColumnsList(address, port, catalog, username, password, tableName);
        return BigQueryUtils.getBigQuerySchema(tableColumns);
	}
	
	public static TableSchema getBigQuerySchema(Connection connection, String tableName) throws SQLException{
		List<MsSqlColumn> tableColumns = getColumnsList(connection, tableName);
        return BigQueryUtils.getBigQuerySchema(tableColumns);
	}
	
}
