package edu.nefu.data_utils;

import java.io.*;

/**
 * 将原始的csv数据中的引号去掉
 */
public class Trim_Tiger {


    public void trimline(String file_in, String file_out) throws Exception{
        File file = new File(file_in);
        FileWriter writer;
        writer = new FileWriter(file_out);
        String encoding = "UTF-8";
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        while ((lineTxt = bufferedReader.readLine()) != null) {
            String replace = lineTxt.replace("\"", "");
            writer.write(replace);
            writer.write("\n");
//            System.out.println(replace);
        }
        writer.flush();
        writer.close();

    }


    public static void main(String[] args) throws Exception {
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/COUNTY.csv","/home/runxuan/data/transform_trim/TIGER/COUNTY.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/LINEARWATER.csv","/home/runxuan/data/transform_trim/TIGER/LINEARWATER.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/AREAWATER.csv","/home/runxuan/data/transform_trim/TIGER/AREAWATER.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/ZCTA5.csv","/home/runxuan/data/transform_trim/TIGER/ZCTA5.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/AREALM.csv","/home/runxuan/data/transform_trim/TIGER/AREALM.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/RAILS.csv","/home/runxuan/data/transform_trim/TIGER/RAILS.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/ROADS.csv","/home/runxuan/data/transform_trim/TIGER/ROADS.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/STATE.csv","/home/runxuan/data/transform_trim/TIGER/STATE.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/EDGES.csv","/home/runxuan/data/transform_trim/TIGER/EDGES.csv");
        new Trim_Tiger().trimline("/home/runxuan/data/transform/TIGER/PRIMARYROADS.csv","/home/runxuan/data/transform_trim/TIGER/PRIMARYROADS.csv");



    }

}
