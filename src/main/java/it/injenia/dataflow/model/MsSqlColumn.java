package it.injenia.dataflow.model;

import java.io.Serializable;

public class MsSqlColumn implements Serializable {
	private String name; // COLUMN_NAME
	private int ordinalPosition; // ORDINAL_POSITION
	private String defaultValue; //COLUMN_DEFAULT
	private boolean nullable; // IS_NULLABLE
	private String dataType; // DATA_TYPE
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getOrdinalPosition() {
		return ordinalPosition;
	}
	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	public boolean isNullable() {
		return nullable;
	}
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	public String getDataType() {
		return dataType;
	}
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	@Override
	public String toString() {
		return "MsSqlColumn [name=" + name + ", ordinalPosition=" + ordinalPosition + ", dataType=" + dataType + "]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof MsSqlColumn))
			return false;
		MsSqlColumn other = (MsSqlColumn) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	
}
