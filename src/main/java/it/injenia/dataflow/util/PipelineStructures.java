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

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import it.injenia.dataflow.model.enumtype.MsSqlDataType;

/**
 * Project structures.
 * 
 * @author Fausto Fusaro
 *
 */
public class PipelineStructures {
	
	public static final Map<MsSqlDataType, String> msSqlToBqTypeMap = ImmutableMap.<MsSqlDataType, String>builder()
		    .put(MsSqlDataType.VARCHAR, "STRING")
		    .put(MsSqlDataType.NVARCHAR, "STRING")
		    .put(MsSqlDataType.CHAR, "STRING")
		    .put(MsSqlDataType.NCHAR, "STRING")
		    .put(MsSqlDataType.TEXT, "STRING")
		    .put(MsSqlDataType.NTEXT, "STRING")
		    .put(MsSqlDataType.BIGINT, "INTEGER")
		    .put(MsSqlDataType.INT, "INTEGER")
		    .put(MsSqlDataType.TINYINT, "INTEGER")
		    .put(MsSqlDataType.SMALLINT, "INTEGER")
		    .put(MsSqlDataType.NUMERIC, "FLOAT")
		    .put(MsSqlDataType.DECIMAL, "FLOAT")
		    .put(MsSqlDataType.MONEY, "FLOAT")
		    .put(MsSqlDataType.SMALLMONEY, "FLOAT")
		    .put(MsSqlDataType.FLOAT, "FLOAT")
		    .put(MsSqlDataType.REAL, "REAL")
		    .put(MsSqlDataType.BIT, "BOOLEAN")
		    .put(MsSqlDataType.DATE, "DATE")
		    .put(MsSqlDataType.TIME, "TIME")
		    .put(MsSqlDataType.DATETIME, "DATETIME")
		    .put(MsSqlDataType.DATETIME2, "DATETIME")
		    .put(MsSqlDataType.DATETIMEOFFSET, "TIMESTAMP")
		    .put(MsSqlDataType.SMALLDATETIME, "DATETIME")
		    .put(MsSqlDataType.TIMESTAMP, "STRING")
		    .put(MsSqlDataType.BINARY, "BYTES")
		    .put(MsSqlDataType.IMAGE, "BYTES")
		    .put(MsSqlDataType.VARBINARY, "BYTES")
		    .put(MsSqlDataType.UNIQUEIDENTIFIER, "STRING")
		    .build(); 
}
