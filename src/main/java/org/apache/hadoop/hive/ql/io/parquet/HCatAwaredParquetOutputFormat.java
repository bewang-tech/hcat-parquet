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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;
import parquet.schema.MessageType;

public class HCatAwaredParquetOutputFormat extends MapredParquetOutputFormat {

	@SuppressWarnings("rawtypes")
	@Override
	public org.apache.hadoop.mapred.RecordWriter getRecordWriter(
			final FileSystem ignored, final JobConf jobConf, final String path,
			final Progressable progress) throws IOException {

		String jobInfoString = jobConf.get(HCatConstants.HCAT_KEY_OUTPUT_INFO);
		Object jobInfo = HCatUtil.deserialize(jobInfoString);

		MessageType parquetSchema = jobInfo instanceof OutputJobInfo ? ParquetSchemaHelper
				.getParquetSchema((OutputJobInfo) jobInfo)
				: DeprecatedParquetSchemaHelper
						.getParquetSchema((org.apache.hcatalog.mapreduce.OutputJobInfo) jobInfo);
		DataWritableWriteSupport.setSchema(parquetSchema, jobConf);

		return new WriterWrapper(getParquetRecordWriter(realOutputFormat,
				jobConf, path, progress));
	}

	@SuppressWarnings("rawtypes")
	private static class WriterWrapper implements
			org.apache.hadoop.mapred.RecordWriter {
		private final org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> writer;

		public WriterWrapper(
				org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> writer) {
			this.writer = writer;
		}

		@Override
		public void write(Object key, Object value) throws IOException {
			try {
				writer.write(null, (ArrayWritable) value);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			try {
				writer.close(null);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> getParquetRecordWriter(
			ParquetOutputFormat<ArrayWritable> realOutputFormat,
			JobConf jobConf, String finalOutPath, Progressable progress)
			throws IOException {

		try {
			// create a TaskInputOutputContext
			TaskAttemptID taskAttemptID = TaskAttemptID.forName(jobConf
					.get("mapred.task.id"));
			if (taskAttemptID == null) {
				taskAttemptID = new TaskAttemptID();
			}
			TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(
					jobConf, taskAttemptID);

			return (org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable>) ((ParquetOutputFormat) realOutputFormat)
					.getRecordWriter(taskContext, new Path(finalOutPath));
		} catch (final InterruptedException e) {
			throw new IOException(e);
		}
	}

}
