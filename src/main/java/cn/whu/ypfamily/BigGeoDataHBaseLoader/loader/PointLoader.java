package cn.whu.ypfamily.BigGeoDataHBaseLoader.loader;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;

/**
 * Point类型输入
 */
public class PointLoader extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new PointLoader(), args);
        System.exit(result);
    }


    public int run(String[] args) throws Exception {
        // 获取输入参数
        if (args.length < 3) {
            System.out.println(
                    "Input " +
                            "*<path in> " +
                            "*<path out> " +
                            "*<table name>"
            );
            return 0;
        }
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        String tableName = args[2];


        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Point Bulk Load");

        job.setJarByClass(PointLoader.class);

        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(TextInputFormat.class);

        Connection hbCon = ConnectionFactory.createConnection(conf);
        Table hTable = hbCon.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = hbCon.getRegionLocator(TableName.valueOf(tableName));
        Admin admin = hbCon.getAdmin();
        int res;

        HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        res = job.waitForCompletion(true) ? 0 : 1;

        hTable.close();
        regionLocator.close();
        admin.close();
        hbCon.close();

        return res;
    }

    protected static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String oid = line[0];

            // 获得GeoHash
            double lon = Double.valueOf(line[1]);
            double lat = Double.valueOf(line[2]);
            String geom_wkt = "Point(" + line[1] + " " + line[2] + ")";
            String geo_hash = GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 12);
            String geom_type = "Point";
            String tags = line[3];

            // 输出
            try {
                Geometry geom = new WKTReader().read(geom_wkt);

                Put p = new Put((geo_hash + "_" + oid).getBytes());
                p.addColumn(geom_type.getBytes(), "the_geom".getBytes(), new WKBWriter().write(geom));
                p.addColumn(geom_type.getBytes(), "oid".getBytes(), oid.getBytes());
                p.addColumn(geom_type.getBytes(), "tags".getBytes(), tags.getBytes());
                context.write(new ImmutableBytesWritable(geo_hash.getBytes()), p);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
