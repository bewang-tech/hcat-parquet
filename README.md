hcat-parquet
============

This is a work around of writing into Parquet table using HCatalog.

You will encounter this error "Should never be used" if you write a Parquet table using HCatalog in Hive 0.12/0.13 in Pig or Sqoop.

The reason is that `MapredParquetOutputFormat.getRecordWriter` will throw a `RuntimeException("Should never be used")`. HCatalog class `org.apache.hive.hcatalog.mapreduce.FileOutputFormatContainer` just simply calls `getBaseOutputFormat().getRecordWriter(...)` which invokes `MapredParquetOutputFormat.getRecordWriter`.

To fix this issue, we can extend `MapredParquetOutputFormat` and override method `getRecordWriter`. Because HCatalog serializes the table schema in `JobConf`, we can get `HCatSchema` from `JobConf`, convert it into Parquet's `MessageType`, set the parquet schema into `JobConf`, then we can use `ParquetOutputFormat.getRecordWriter(taskContext, outputPath)` to get the real record writer. This is class `HCatAwaredParquetOutputFormat`.

Another issue is: Sqoop 1 uses deprecated API (`org.apache.hcatalog.mapreduce.OutputJobInfo`), but Pig uses the new HCatalog API (e.g, `org.apache.hive.hcatalog.mapreduce.OutputJobInfo`). `HCatAwaredParquetOutputFormat` needs to handle the HCatalog Schema serialized by different API. See `DeprecatedParquetSchemaHelper` and `ParquetSchemaHelper`.

Here is how to use this outputformat. For the parquet table `my_table`, define a helper table `my_table_hcat_pq` with outputformat replaced with `HCatAwaredParquetOutputFormat`. And two tables share the same location. You can read/write table through the normal table `my_table` using Hive and use `my_table_hcat_pq` only when you want to write the table in Pig or Sqoop through HCatalog. You need to put `hcat-parquet.jar` in `HADOOP_CLASSPATH`.
```
$ export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$a_path/hcat-parquet.jar
$ hive
hive> create external table my_table (id int, name string)
      stored as Parquet 
      location 'table_path';
hive> create external table my_table_hcat_pq (id int, name string)
      ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      STORED AS 
        INPUTFORMAT 
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 
          'org.apache.hadoop.hive.ql.io.parquet.HCatAwaredParquetOutputFormat'
      location 'table_path';
```
