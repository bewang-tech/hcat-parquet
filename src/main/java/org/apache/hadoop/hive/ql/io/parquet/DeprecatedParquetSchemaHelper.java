package org.apache.hadoop.hive.ql.io.parquet;

import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.FLOAT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.INT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SMALLINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.TINYINT_TYPE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

import parquet.schema.MessageType;

public class DeprecatedParquetSchemaHelper {

	public static MessageType getParquetSchema(OutputJobInfo jobInfo)
			throws IOException {
		HCatSchema columns = jobInfo.getTableInfo().getDataColumns();

		int size = columns.size();
		List<String> columnNames = columns.getFieldNames();
		List<TypeInfo> columnTypes = getColumnTypes(columns, size);

		MessageType parquetSchema = HiveSchemaConverter.convert(columnNames,
				columnTypes);
		return parquetSchema;
	}

	public static List<TypeInfo> getColumnTypes(HCatSchema columns, int size) {
		List<TypeInfo> columnTypes = new ArrayList<>(size);

		for (HCatFieldSchema fs : columns.getFields()) {
			switch (fs.getType()) {
			case INT:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(INT_TYPE_NAME));
				break;
			case TINYINT:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(TINYINT_TYPE_NAME));
				break;
			case SMALLINT:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(SMALLINT_TYPE_NAME));
				break;
			case BIGINT:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(BIGINT_TYPE_NAME));
				break;
			case BOOLEAN:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(BOOLEAN_TYPE_NAME));
				break;
			case FLOAT:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(FLOAT_TYPE_NAME));
				break;
			case DOUBLE:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(DOUBLE_TYPE_NAME));
				break;
			case STRING:
				columnTypes.add(TypeInfoFactory
						.getPrimitiveTypeInfo(STRING_TYPE_NAME));
				break;
			default:
				throw new UnsupportedOperationException("Type " + fs.getType()
						+ " is not supported.");
			}
		}
		return columnTypes;
	}

}
