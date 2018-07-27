1、在HBase shell中创建数据表

`create '表名', {NAME=>'Polygon/Polyline/Point'}`

2、将要处理的tsv格式数据存储到HDFS上

3、运行MapReduce程序生成HFile

`hadoop -jar BigGeoDataHbaseLoader.jar class路径 -Dhbase.zookeeper.quorum=zookeeper集群地址 输入空间数据tsv文件路径 输出HFile路径 空间数据表名`

4、将HFile添加到HBase表中

`hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles HFile路径 空间数据表名`
