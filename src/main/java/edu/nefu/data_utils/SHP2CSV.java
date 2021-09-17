package edu.nefu.data_utils;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SHP2CSV {
    public void shptoCsv(String shpPath, String csvPath)throws Exception{
        int size = 0;
        BufferedWriter out =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvPath),"UTF-8"));
        File file = new File(shpPath);
        ShapefileDataStore shpDataStore = null;
        shpDataStore = new ShapefileDataStore(file.toURL());
        shpDataStore.setCharset(Charset.forName("gbk"));
        String typeName = shpDataStore.getTypeNames()[0];
        SimpleFeatureSource featureSource = null;
        featureSource =  shpDataStore.getFeatureSource (typeName);
        SimpleFeatureCollection result = featureSource.getFeatures();


        Map<String, Class> mapFields = new HashMap();
        List<AttributeType> attributeTypes = result.getSchema().getTypes();
//        System.out.println(result.getSchema().getGeometryDescriptor().getType().getName());

        // geometry的类型
        String geometryType = result.getSchema().getGeometryDescriptor().getType().getName().toString();


        List<String> csvHeader = new ArrayList<>();
        for (int i=0;i<attributeTypes.size();i++){
            AttributeType attributeType = attributeTypes.get(i);
            String key = attributeType.getName().toString();
            Class classType = attributeType.getBinding();
            if (geometryType.equals(key)){
                continue;
            }
            csvHeader.add(key);
            out.write(key + ",");
            mapFields.put(key,classType);
        }
        out.write("coords");

        out.newLine();
        SimpleFeatureIterator itertor = result.features();
        while (itertor.hasNext()) {
            size++;
            SimpleFeature feature = itertor.next();
//            System.out.println(feature);
            String coords = feature.getDefaultGeometry().toString();
            out.write(",");
//            if(coords.contains(",")){
////                coords = "\"" + coords + "\"";
//                coords = coords;
//            }
            out.write(coords);
            for(int i=0;i<csvHeader.size();i++){
                String key = csvHeader.get(i);
                String classType = mapFields.get(key).getTypeName();
                String oneCsvData = feature.getAttribute(csvHeader.get(i)).toString();

                if (classType.contains("Double")){
                    BigDecimal bd = new BigDecimal(oneCsvData);
                    oneCsvData = bd.toPlainString();
                }

                if(oneCsvData.contains(",")){
                    oneCsvData = "\"" + oneCsvData + "\"";
                }

                out.write(oneCsvData);
            }


            out.newLine();

        }
        System.out.println(size);
//        itertor.close();
        out.flush();

    }

    public static void main(String[] args) throws Exception {
        SHP2CSV shp2CSV = new SHP2CSV();
        long startTime = System.currentTimeMillis();
        System.out.println(startTime);
        shp2CSV.shptoCsv("/home/runxuan/data/shapefile/虎豹国家公园/2017林地变更图/虎豹2.shp","/home/runxuan/data/shapefile/hb.csv");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-startTime);

    }






}
