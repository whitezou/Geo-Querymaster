package edu.nefu.data_utils;

import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;

public class OSM_Mask2_dumplicate {
    public void maskOSMPoint(String osmPath, String shpPath, String file_out, String geo_type) throws Exception {
        File file = new File(osmPath);
        FileWriter writer;
        writer = new FileWriter(file_out);
        String encoding = "UTF-8";
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        DefaultFeatureCollection states = getCollection(shpPath, geo_type);
        FeatureIterator features = states.features();
        Geometry intersection = (Geometry) ((SimpleFeature) features.next()).getDefaultGeometry();

        while (features.hasNext()) {
            SimpleFeature next = (SimpleFeature) features.next();
            Geometry defaultGeometry = (Geometry) next.getDefaultGeometry();
            intersection = intersection.union(defaultGeometry);
        }
        Envelope envelopeInternal = intersection.getEnvelopeInternal();
        System.out.println(envelopeInternal);
        //对interesection进行可视化
/*        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("TwoDistancesType");
        builder.setCRS(DefaultGeographicCRS.WGS84);
//            builder.add("MultiPolygon", MultiPolygon.class);
        if ("multipolygon".equals(geo_type)) {
            builder.add("MultiPolygon", MultiPolygon.class);
        }
        if ("multipolyline".equals(geo_type)) {
            builder.add("line", LineString.class);
        }
        if ("linestring".equals(geo_type)) {
            builder.add("line", LineString.class);
        }
        if ("point".equals(geo_type)) {
            builder.add("Point", Point.class);
        }
        final SimpleFeatureType TYPE = builder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection("internal", TYPE);
        featureBuilder.add(intersection);
        SimpleFeature feature = featureBuilder.buildFeature(null);
        featureCollection.add(feature);
        SimpleFeatureSource featuresource = DataUtilities.source(featureCollection);
//                Color styleColor=new Color(255,0,0);
        Color styleColor1=new Color(255,0,0);
        Style style = SLD.createSimpleStyle(featuresource.getSchema(),styleColor1);
        FeatureLayer layer = new FeatureLayer(featuresource, style);
        MapContent map = new MapContent();
        map.addLayer(layer);
        JMapFrame.showMap(map);*/


//        GeometryFactory geometryFactory = new GeometryFactory();
//        Geometry envelope_Geometry = geometryFactory.toGeometry(states.getBounds());

        WKTReader wktReader = new WKTReader();
        while ((lineTxt = bufferedReader.readLine()) != null) {
//            FeatureIterator features = states.features();
            Geometry point = wktReader.read(lineTxt);

            if (intersection.disjoint(point)) {
//                System.out.println(lineTxt);
                writer.write(lineTxt);
                writer.write("\n");
            }
//            System.out.println(states.contains(point));
//            System.out.println(point);
//            while (features.hasNext()) {
//                SimpleFeature next = (SimpleFeature) features.next();
//                Geometry defaultGeometry = (Geometry) next.getDefaultGeometry();
//
//                if (!(defaultGeometry.disjoint(point))) {
//                    System.out.println(lineTxt);
//                    writer.write(lineTxt);
//                    writer.write("\n");
//                    break;
//                }
////                System.out.println(next);
//            }
        }
        writer.flush();
        writer.close();

    }

    public DefaultFeatureCollection /*void*/ getCollection(String Path, String geo_type) throws Exception {
        File file = new File(Path);
        String encoding = "UTF-8";
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("TwoDistancesType");
        builder.setCRS(DefaultGeographicCRS.WGS84);

        if ("multipolygon".equals(geo_type)) {
            builder.add("MultiPolygon", MultiPolygon.class);
        }
        if ("multipolyline".equals(geo_type)) {
            builder.add("line", LineString.class);
        }
        if ("point".equals(geo_type)) {
            builder.add("Point", Point.class);
        }
        final SimpleFeatureType TYPE = builder.buildFeatureType();
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(TYPE);
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection("internal", TYPE);

        WKTReader wktReader = new WKTReader();
        while ((lineTxt = bufferedReader.readLine()) != null) {
            String wkt = lineTxt.split("\t")[0];
            Geometry geometry = wktReader.read(wkt);

//            System.out.println(geometry);
//            System.out.println(geometry);
            featureBuilder.add(geometry);
            SimpleFeature feature = featureBuilder.buildFeature(null);
            featureCollection.add(feature);
        }
//        featureCollection.features().next().getDefaultGeometry()
        return featureCollection;

    }

    public static void main(String[] args) throws Exception {
        new OSM_Mask2_dumplicate().maskOSMPoint("/home/runxuan/data/transform_trim/mask_all_node/all_nodes_temp.csv",
                "/home/runxuan/data/transform_trim/TIGER/STATE.csv",
                "/home/runxuan/data/transform_trim/mask_all_node/all_nodes_mask.csv",
                "multipolygon");
//        new OSM_Mask().getCollection("/home/runxuan/data/transform_trim/TIGER/STATE.csv", "multipolygon");

    }

}
