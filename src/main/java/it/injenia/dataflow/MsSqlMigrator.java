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

package it.injenia.dataflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

import it.injenia.dataflow.model.DbRow;
import it.injenia.dataflow.model.MsSqlColumn;
import it.injenia.dataflow.model.MsSqlTable;
import it.injenia.dataflow.model.enumtype.MsSqlDataType;
import it.injenia.dataflow.transform.BigQueryTableDestination;
import it.injenia.dataflow.transform.TableToDbRowFn;
import it.injenia.dataflow.util.MsSqlManager;

/**
 * This is the main class of the pipeline, developed with Dataflow SDK 2.1.0.
 * Basically this pipeline will migrate a MS SQL Server catalog to a dataset
 * BigQuery. You need to prepare your MS SQL Server instance and BigQuery
 * project parameters.
 * 
 * <p>
 * Then just inject them via launch options as explained here:
 * 
 * <pre>
 * {@code
 * - Project: Google Cloud Console project onto deploy this pipeline and where reside BigQuery output dataset
 * - Temp Location: a Google Cloud Storage path for Dataflow to stage any temporary files; the bucket must exists before the run
 * - Staging Location: a Google Cloud Storage path for Dataflow to stage binary/compiled files; the bucket must exists before the run
 * - MS SQL address reachable from the pipeline when it will run on Dataflow, such as VM external IP address
 * - MS SQL address reachable from your launcher machine
 * - MS SQL catalog name to migrate
 * - MS SQL username and password
 * - BigQuery output dataset name: this dataset must be created before the run
 * }
 * </pre>
 * </p>
 * 
 * <p>
 * Regardless of you are running locally or on Dataflow, you have to set these
 * parameters:
 * 
 * <pre>
 * {@code
 * 	--project=[google cloud project mnemonic id]
 * 	--tempLocation=gs://[bucket]/[path] 
 * 	--stagingLocation=gs://[bucket]/[path] 
 * 	--sqlAddressFromLauncher=[SQL instance external address] 
 * 	--sqlPort=[*optional SQL port, default 1433] 
 * 	--sqlUsername=[username] 
 * 	--sqlPassword=[password]
 * 	--sqlCatalog=[catalog name]
 * 	--bigQueryDataset=[bq dataset name]
 * }
 * </pre>
 * 
 * If you run locally:
 * 
 * <pre>
 * {@code
 * 	--runner=DirectRunner
 *  --sqlAddressFromPipeline=[SQL instance external address]
 * }
 * </pre>
 * 
 * If you run on Dataflow:
 * 
 * <pre>
 * {@code
 * 	--runner=DataflowRunner
 *  --sqlAddressFromPipeline=[SQL instance address visible from Dataflow, maybe an internal address]
 * }
 * </pre>
 * </p>
 * 
 * <p>
 * As you can see, the main difference is the address of SQL Server. The value
 * of sqlAddressFromPipeline is often the same to the value of
 * sqlAddressFromLauncher. Sometimes you could specify an internal VPN network
 * address when Dataflow uses a SQL instance installed on a GCE VM. This is an
 * optimization because VPC internal traffic is more efficient then using the
 * external address.
 * </p>
 * 
 * @author Fausto Fusaro
 *
 */
public class MsSqlMigrator {
	// Using SLF4J Logger. If you run on Dataflow it will use Google Stackdriver
	// Logging
	private static final Logger _log = LoggerFactory.getLogger(MsSqlMigrator.class);

	/**
	 * This interface specifies all options you need to run the pipeline. See
	 * class documentation.
	 *
	 */
	public interface MigratorOptions extends PipelineOptions {

		@Description("MS SQL Server address reachable from the launcher machine")
		@Required
		String getSqlAddressFromLauncher();

		void setSqlAddressFromLauncher(String value);

		@Description("MS SQL Server address reachable from the pipeline")
		@Required
		String getSqlAddressFromPipeline();

		void setSqlAddressFromPipeline(String value);

		@Description("MS SQL Server port number")
		@Default.Integer(1433)
		@Required
		Integer getSqlPort();

		void setSqlPort(Integer value);

		@Description("MS SQL Server source catalog name")
		@Required
		String getSqlCatalog();

		void setSqlCatalog(String value);

		@Description("MS SQL Server username")
		@Default.String("migrator")
		@Required
		String getSqlUsername();

		void setSqlUsername(String value);

		@Description("MS SQL Server password")
		@Required
		String getSqlPassword();

		void setSqlPassword(String value);

		@Description("BigQuery dataset name")
		@Required
		String getBigQueryDataset();

		void setBigQueryDataset(String value);
	}

	/**
	 * Main method. You must specify this as the method to invoke as first.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// reads options from launcher
		MigratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MigratorOptions.class);

		// creates the pipeline
		Pipeline pipeline = Pipeline.create(options);

		// We need to collect SQL catalog data to orchestrate steps below.
		// This is an hash map with each table object as key and the ordered
		// list of column as value
		final Map<MsSqlTable, List<MsSqlColumn>> tableMap = new HashMap<MsSqlTable, List<MsSqlColumn>>();

		Connection connection = null;
		try {
			connection = MsSqlManager.getConnection(options.getSqlAddressFromLauncher(), options.getSqlPort(),
					options.getSqlCatalog(), options.getSqlUsername(), options.getSqlPassword());
			List<MsSqlTable> tables = MsSqlManager.getTablesList(connection);
			for (MsSqlTable table : tables) {
				_log.debug("Extracting table schema: " + table.getFullName());

				List<MsSqlColumn> tableColumns = MsSqlManager.getColumnsList(connection, table.getName());
				tableMap.put(table, tableColumns);
			}
			connection.close();
		} catch (SQLException e) {
			_log.error("Failed to prepare tables map.", e);
			try {
				connection.close();
			} catch (SQLException e1) {
			}
			System.exit(1);
		}

		// creates a PCollection with the map of tables and columns
		PCollectionView<Map<MsSqlTable, List<MsSqlColumn>>> columnNamesCollection = pipeline
				.apply("Collect catalog map", Create.of(tableMap))
				.apply("Catalog map serialization", View.<MsSqlTable, List<MsSqlColumn>> asMap());

		Set<MsSqlTable> tables = tableMap.keySet();

		// creates a PCollection with the the list of tables
		PCollection<MsSqlTable> tableCollection = pipeline.apply("Creates table list", Create.of(tables));

		// creates a PCollection of pairs [table] - [a row in this table]
		PCollection<KV<MsSqlTable, DbRow>> dbRowKeyValue = tableCollection.apply("SQL table to DbRow(s)", ParDo
				.of(new TableToDbRowFn.Builder(MsSqlManager.getDriver(),
						MsSqlManager.getConnectionString(options.getSqlAddressFromPipeline(), options.getSqlPort(),
								options.getSqlCatalog())).withUsername(options.getSqlUsername())
										.withPassword(options.getSqlPassword()).withColumnsMap(columnNamesCollection)
										.create())
				.withSideInputs(columnNamesCollection));

		// this is the end of the pipeline. It writes each row in his correspondent table in BigQuery output dataset
		dbRowKeyValue.apply("BigQuery output",
				BigQueryIO.<KV<MsSqlTable, DbRow>> write()
						.to(new BigQueryTableDestination(tableMap, options.getBigQueryDataset()))
						.withFormatFunction(new SerializableFunction<KV<MsSqlTable, DbRow>, TableRow>() {

							@Override
							public TableRow apply(KV<MsSqlTable, DbRow> kv) {

								MsSqlTable table = kv.getKey();
								DbRow dbRow = kv.getValue();
								List<MsSqlColumn> columnNames = tableMap.get(table);

								List<Object> fields = dbRow.fields();
								TableRow bqRow = new TableRow();
								for (int i = 0; i < fields.size(); i++) {
									Object fieldData = fields.get(i);
									if (fieldData == null)
										continue;

									MsSqlColumn column = columnNames.get(i);
									String columnName = column.getName();

									String sFieldData = fieldData.toString();
									// if (column.getDataType())
									_log.debug(String.format("SerializableFunction: %s [%s] %s", table.getFullName(),
											column.getDataType(), sFieldData));

									if (column.getDataType().equalsIgnoreCase(MsSqlDataType.IMAGE.toString())) {
										_log.debug("I'm converting an image field");
										byte[] bytesEncoded = Base64.encodeBase64(sFieldData.getBytes());
										bqRow.put(columnName, bytesEncoded);
										continue;
									}

									if (!sFieldData.toLowerCase().equals("null"))
										bqRow.put(columnName, sFieldData);
								}

								return bqRow;
							}
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		pipeline.run();
	}
}
