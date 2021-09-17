package edu.nefu.data_utils;
/**
 * 将阿拉斯加地区的数据去掉
 */

import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.*;

public class DataProcess {


    public String getOSMwkt(String lineTxt) {
        String wkt = lineTxt.split("  ")[1].replace("\"", "");
        return wkt;
    }

    public String getTIGERwkt(String lineTxt) {
        String wkt = lineTxt.split("\t")[0].replace("\"", "");
        return wkt;
    }

    public void addShapeLayer(String file_in, String file_out) {
        try {
            Envelope envelope = new Envelope(-124.848974, -66.885444, 24.396308, 49.384358);
            GeometryFactory geometryFactory = new GeometryFactory();
            Geometry envelope_Geometry = geometryFactory.toGeometry(envelope);
//            System.out.println(envelope_Geometry);
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            String encoding = "UTF-8";
            File file = new File(file_in);
            FileWriter writer;
            writer = new FileWriter(file_out);
            WKTReader wktReader = new WKTReader();
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;

                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String wkt = lineTxt.split("\t")[0].replace("\"", "");
                    Geometry geometry = wktReader.read(wkt);
//                    System.out.println("envelope:"+geometry.getEnvelopeInternal());
//                    System.out.println(lineTxt);
                    if (!(geometry.disjoint(envelope_Geometry))) {
//                        System.out.println(geometry.getEnvelopeInternal());
//                        System.out.println(lineTxt);
                        writer.write(lineTxt);
                        writer.write("\n");
                    }
                }
                writer.flush();
                writer.close();

            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ParseException {


        DataProcess dataProcess = new DataProcess();
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/COUNTY.csv", "/home/runxuan/data/transform/COUNTY.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/LINEARWATER.csv", "/home/runxuan/data/transform/LINEARWATER.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/AREAWATER.csv", "/home/runxuan/data/transform/AREAWATER.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/ZCTA5.csv", "/home/runxuan/data/transform/ZCTA5.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/AREALM.csv", "/home/runxuan/data/transform/AREALM.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/RAILS.csv", "/home/runxuan/data/transform/RAILS.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/ROADS.csv", "/home/runxuan/data/transform/ROADS.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/STATE.csv", "/home/runxuan/data/transform/STATE.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/EDGES.csv", "/home/runxuan/data/transform/EDGES.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/TIGER/PRIMARYROADS.csv", "/home/runxuan/data/transform/PRIMARYROADS.csv");
//        String OSMString = "192056  POLYGON ((8.7599721 49.7103028, 8.759997 49.7102752, 8.7600145 49.7102818, 8.7600762 49.7102133, 8.760178 49.7102516, 8.7600914 49.7103478, 8.7599721 49.7103028))      [type#multipolygon,name#Friedhof Kolmbach,landuse#cemetery]\n";
//        String TigerString = "\"POLYGON ((-83.576634 41.60105,-83.577534 41.60105,-83.580934 41.60135,-83.581734 41.60135,-83.583734 41.60115,-83.585534 41.60105,-83.588034 41.60095,-83.584534 41.60435,-83.582034 41.60505,-83.578334 41.61095,-83.569934 41.61715,-83.565729 41.617463,-83.56562 41.617471,-83.563882 41.617601,-83.563761 41.61761,-83.563233 41.61765,-83.559833 41.61765,-83.559758 41.61765,-83.559605 41.61765,-83.556936 41.61765,-83.556253 41.61765,-83.553433 41.61765,-83.551233 41.61775,-83.549927 41.617746,-83.548621 41.617841,-83.548531 41.617835,-83.548455 41.61783,-83.54622 41.61769,-83.544833 41.617882,-83.543622 41.617898,-83.542989 41.617904,-83.54303 41.614927,-83.543034 41.614741,-83.542933 41.61435,-83.543033 41.61405,-83.542933 41.61355,-83.542933 41.61295,-83.542934 41.612807,-83.542934 41.60973,-83.542923 41.608893,-83.543343 41.60889,-83.544129 41.608883,-83.546064 41.608861,-83.546497 41.608846,-83.546769 41.60882,-83.546908 41.6088,-83.547039 41.608771,-83.547393 41.608675,-83.547612 41.608603,-83.547875 41.608497,-83.548075 41.6084,-83.548264 41.608291,-83.548484 41.60814,-83.54873 41.607944,-83.549976 41.606838,-83.550538 41.606366,-83.552187 41.604925,-83.552425 41.604726,-83.552601 41.604582,-83.553881 41.603451,-83.555696 41.601851,-83.555966 41.601613,-83.555433 41.601436,-83.55488 41.601388,-83.55423 41.601412,-83.553676 41.601653,-83.553482 41.60185,-83.548442 41.601849,-83.549179 41.600962,-83.548772 41.600957,-83.549571 41.599995,-83.552387 41.596599,-83.554406 41.594164,-83.55443 41.594135,-83.555054 41.593382,-83.555543 41.592815,-83.555838 41.592492,-83.555842 41.592333,-83.555921 41.586576,-83.556093 41.58658,-83.556302 41.586595,-83.556411 41.586614,-83.556631 41.58667,-83.556739 41.586708,-83.556959 41.586798,-83.557077 41.586833,-83.557197 41.586859,-83.557321 41.586875,-83.557508 41.586887,-83.557764 41.586885,-83.557763 41.586776,-83.557763 41.585926,-83.557771 41.585426,-83.55778 41.585281,-83.55778 41.585101,-83.557836 41.582262,-83.557853 41.580731,-83.557853 41.580639,-83.557852 41.580559,-83.567166 41.580676,-83.567448 41.580692,-83.567447 41.58119,-83.567447 41.581439,-83.567446 41.581781,-83.567346 41.58186,-83.567128 41.582074,-83.566516 41.582628,-83.566348 41.582783,-83.566277 41.582849,-83.56617 41.582948,-83.564485 41.584507,-83.56343 41.58547,-83.562304 41.586499,-83.561879 41.586887,-83.562328 41.586888,-83.562733 41.586515,-83.563844 41.58651,-83.563844 41.586434,-83.56385 41.586208,-83.563874 41.586028,-83.563908 41.585896,-83.563957 41.585766,-83.564022 41.58564,-83.564101 41.585518,-83.56414 41.58547,-83.56518 41.584517,-83.565781 41.58452,-83.56745 41.584582,-83.567443 41.585024,-83.567449 41.586373,-83.567443 41.586886,-83.568358 41.586886,-83.573433 41.586887,-83.575085 41.586882,-83.57529 41.586881,-83.576195 41.586882,-83.576237 41.586883,-83.575986 41.587498,-83.575802 41.587892,-83.575715 41.58816,-83.575433 41.58885,-83.575633 41.58975,-83.574933 41.58975,-83.574778 41.590144,-83.57695 41.590149,-83.577242 41.590186,-83.577236 41.59097,-83.577238 41.591273,-83.576843 41.591554,-83.576419 41.591861,-83.57628 41.591916,-83.575279 41.592262,-83.574694 41.592399,-83.573992 41.592525,-83.573831 41.592545,-83.5745 41.590853,-83.568676 41.59097,-83.568831 41.592945,-83.568833 41.593271,-83.568846 41.593525,-83.56884 41.593854,-83.568846 41.594115,-83.568858 41.594586,-83.56973 41.594586,-83.569732 41.59391,-83.569733 41.59385,-83.573425 41.593853,-83.574106 41.593853,-83.572655 41.594907,-83.57223 41.595212,-83.573512 41.595231,-83.574776 41.596044,-83.574578 41.596193,-83.574372 41.596351,-83.573861 41.596742,-83.572349 41.597886,-83.571329 41.598657,-83.570659 41.599173,-83.569425 41.600109,-83.574133 41.59995,-83.574133 41.60115,-83.574633 41.60105,-83.576634 41.60105))\"       43460   43460   B5      G6350   S       7158544 791729  +41.6011920     -083.5649985\n";

//        String osMwkt = dataProcess.getTIGERwkt(TigerString);

//        String osMwkt = dataProcess.getOSMwkt(OSMString);
//        System.out.println(osMwkt);
//        WKTReader wktReader = new WKTReader();
//        Geometry read = wktReader.read(osMwkt);
//
        dataProcess.addShapeLayer("/home/runxuan/data/transform_trim/mask_all_node/all_nodes_temp.csv",
                "/home/runxuan/data/transform_trim/mask_all_node/all_nodes_temp2.csv");
//        dataProcess.addShapeLayer("/home/runxuan/data/Spatial_Compare/tiger_rectangles.csv",
//                "/home/runxuan/data/Spatial_Compare/USA/tiger_rectangles.csv");
//        System.out.println(read.getEnvelopeInternal());

    }

}
