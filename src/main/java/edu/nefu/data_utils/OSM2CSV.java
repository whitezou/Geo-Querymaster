package edu.nefu.data_utils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import java.io.*;

public class OSM2CSV {
    public void trans_Point(String inPath, String outPath) throws Exception {
        File file = new File(inPath);
        FileWriter writer = new FileWriter(outPath);
        String encoding = "UTF-8";
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        GeometryFactory geometryFactory = new GeometryFactory();

        while ((lineTxt = bufferedReader.readLine()) != null) {

            String[] splitString = lineTxt.split("\t");
//            System.out.println(splitString.length);
            double x = Double.parseDouble(splitString[1]);
            double y = Double.parseDouble(splitString[2]);
            Point point = geometryFactory.createPoint(new Coordinate(x, y));
            writer.write(point.toString());
            writer.write("\t");
            writer.write(splitString[3]);
            writer.write("\n");
        }
        writer.flush();
        writer.close();


    }

    public void trans_Polygon(String inPath, String outPath) throws Exception {
        File file = new File(inPath);
        FileWriter writer = new FileWriter(outPath);
        String encoding = "UTF-8";
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;

        while ((lineTxt = bufferedReader.readLine()) != null) {

            String[] splitString = lineTxt.split("\t");
            writer.write(splitString[1]);
            writer.write("\t");
            writer.write(splitString[2]);
            writer.write("\n");

        }
        writer.flush();
        writer.close();

    }

    public static void main(String[] args) throws Exception {
        OSM2CSV osm2CSV = new OSM2CSV();
//        osm2CSV.trans_Point("/home/runxuan/data/OSM/all_nodes","/home/runxuan/data/transform_trim/OSM/all_nodes.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/all_objects","/home/runxuan/data/transform_trim/OSM/all_objects.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/buildings","/home/runxuan/data/transform_trim/OSM/buildings.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/cemetery","/home/runxuan/data/transform_trim/OSM/cemetery.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/lakes","/home/runxuan/data/transform_trim/OSM/lakes.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/parks","/home/runxuan/data/transform_trim/OSM/parks.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/postal_codes","/home/runxuan/data/transform_trim/OSM/postal_codes.csv");
//        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/road_network", "/home/runxuan/data/transform_trim/OSM/road_network.csv");
        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/roads", "/home/runxuan/data/transform_trim/OSM/roads.csv");
        osm2CSV.trans_Polygon("/home/runxuan/data/OSM/sports", "/home/runxuan/data/transform_trim/OSM/sports.csv");

    }

}
