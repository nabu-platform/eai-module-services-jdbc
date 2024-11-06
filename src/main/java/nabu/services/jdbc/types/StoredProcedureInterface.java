/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package nabu.services.jdbc.types;

import java.sql.DatabaseMetaData;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import be.nabu.eai.repository.util.KeyValueMapAdapter;

public class StoredProcedureInterface {
	
	private List<StoredProcedureParameter> parameters;
	
	public List<StoredProcedureParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<StoredProcedureParameter> parameters) {
		this.parameters = parameters;
	}

	public enum ParameterType {
		UNKNOWN(DatabaseMetaData.procedureColumnUnknown),
		IN(DatabaseMetaData.procedureColumnIn),
		IN_OUT(DatabaseMetaData.procedureColumnInOut),
		OUT(DatabaseMetaData.procedureColumnOut),
		RETURN(DatabaseMetaData.procedureColumnReturn),
		RESULT(DatabaseMetaData.procedureColumnResult);
		
		private int jdbcValue;

		private ParameterType(int jdbcValue) {
			this.jdbcValue = jdbcValue;
		}
		public static ParameterType fromJdbcValue(int value) {
			for (ParameterType type : values()) {
				if (type.jdbcValue == value) {
					return type;
				}
			}
			return null;
		}
	}
	
	public static class StoredProcedureParameter {
		private ParameterType parameterType;
		private String name, description, procedure;
		private int sqlType, precision, length, scale, radix;
		private boolean nullable;
		private Map<String, String> properties;
		public ParameterType getParameterType() {
			return parameterType;
		}
		public void setParameterType(ParameterType parameterType) {
			this.parameterType = parameterType;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getDescription() {
			return description;
		}
		public void setDescription(String description) {
			this.description = description;
		}
		public int getSqlType() {
			return sqlType;
		}
		public void setSqlType(int sqlType) {
			this.sqlType = sqlType;
		}
		public int getPrecision() {
			return precision;
		}
		public void setPrecision(int precision) {
			this.precision = precision;
		}
		public int getLength() {
			return length;
		}
		public void setLength(int length) {
			this.length = length;
		}
		public int getScale() {
			return scale;
		}
		public void setScale(int scale) {
			this.scale = scale;
		}
		public int getRadix() {
			return radix;
		}
		public void setRadix(int radix) {
			this.radix = radix;
		}
		public boolean isNullable() {
			return nullable;
		}
		public void setNullable(boolean nullable) {
			this.nullable = nullable;
		}
		public String getProcedure() {
			return procedure;
		}
		public void setProcedure(String procedure) {
			this.procedure = procedure;
		}
		@XmlJavaTypeAdapter(value = KeyValueMapAdapter.class)
		public Map<String, String> getProperties() {
			return properties;
		}
		public void setProperties(Map<String, String> parameters) {
			this.properties = parameters;
		}
	}
}
