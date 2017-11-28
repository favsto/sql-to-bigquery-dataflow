package it.injenia.dataflow.model;

import java.io.Serializable;
import java.util.List;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import com.google.auto.value.AutoValue;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class DbRow implements Serializable {
	
	public abstract List<Object> fields();
	
	public static DbRow create(List<Object> fields) {
		return new AutoValue_DbRow(fields);
	}
	
}
