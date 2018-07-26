package cn.whu.ypfamily.BigGeoDataHBaseLoader.util;

import ch.hsr.geohash.GeoHash;
import org.apache.hadoop.hbase.client.Put;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import java.util.HashMap;
import java.util.Map;

public class LoaderUtil {

    /**
     * sv中的一行数据转为HBase的输出
     * @param line 输入行
     * @param geom_type 几何对象类型
     * @return 输出键值对
     */
    public static Map<String, Object> loadLine(String line, String geom_type, String wkt_type) {
        // 获取值
        String[] values = line.split("\t");
        String geom_wkt = values[0];
        String oid = values[1];
        String tags = values[2];
        Map<String, Object> m = new HashMap<String, Object>();
        if (geom_wkt.length() < 1 || !geom_wkt.contains(wkt_type) || geom_wkt.contains("EMPTY")) {
            return null;
        }

        // 生成HBase输出
        try {
            // 验证几何对象是否有效
            Geometry geom = new WKTReader().read(geom_wkt);
            if (geom == null) {
                return null;
            }
            if (!geom.isValid()) {
                geom = geom.buffer(0);
            }
            // 获取GeoHash编码
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
            // 获取输出
            Put p = new Put(geo_rowKey.getBytes());
            p.addColumn(geom_type.getBytes(), "the_geom".getBytes(), new WKBWriter().write(geom));
            p.addColumn(geom_type.getBytes(), "oid".getBytes(), oid.getBytes());
            p.addColumn(geom_type.getBytes(), "tags".getBytes(), tags.getBytes());
            m.put("key", geo_rowKey);
            m.put("value", p);
            return m;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
