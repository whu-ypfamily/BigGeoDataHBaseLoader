package cn.whu.ypfamily.HBaseBulkLoad;

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
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;

public class PolygonBulkLoader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int result = ToolRunner.run(new Configuration(), new PolygonBulkLoader(), args);
        System.exit(result);
    }

    public int run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        String tableName = args[2];

        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "Polygon Bulk Load");

        job.setJarByClass(PolygonBulkLoader.class);

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
            String geom_wkt = line[0];
            String oid = line[1];
            String tags = line[2];
            String geom_type = "Polygon";
            if (geom_wkt.length() < 1 || !geom_wkt.contains("POLYGON") || geom_wkt.contains("EMPTY")) {
                return;
            }

            try {
                Geometry geom = new WKTReader().read(geom_wkt);
                if (geom == null) {
                    return;
                }

                Envelope env = geom.getEnvelopeInternal();
                Double minX = env.getMinX();
                Double maxX = env.getMaxX();
                Double minY = env.getMinY();
                Double maxY = env.getMaxY();

                String[] arrGeoHashes = {
                        GeoHash.geoHashStringWithCharacterPrecision(minY, minX, 12),
                        GeoHash.geoHashStringWithCharacterPrecision(minY, maxX, 12),
                        GeoHash.geoHashStringWithCharacterPrecision(maxY, maxX, 12),
                        GeoHash.geoHashStringWithCharacterPrecision(maxY, minX, 12)
                };
                String strGeoHash = TextUtil.longestCommonPrefix(arrGeoHashes);

                String geo_rowKey = strGeoHash + "_" + oid;

                Put p = new Put(geo_rowKey.getBytes());
                p.addColumn(geom_type.getBytes(), "the_geom".getBytes(), geom_wkt.getBytes());
                p.addColumn(geom_type.getBytes(), "oid".getBytes(), oid.getBytes());
                p.addColumn(geom_type.getBytes(), "tags".getBytes(), tags.getBytes());

                context.write(new ImmutableBytesWritable(geo_rowKey.getBytes()), p);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }
}
