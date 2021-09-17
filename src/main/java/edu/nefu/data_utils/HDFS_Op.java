package edu.nefu.data_utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFS_Op {
    public static void upload(String localPath, String HDFSpath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://219.217.203.1:9000");
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(localPath), new Path(HDFSpath));
    }

    public static void removeFile(String path) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://219.217.203.1:9000");
        FileSystem fs = FileSystem.newInstance(conf);
        fs.delete(new Path(path), true);
    }

    public static void main(String[] args) throws IOException {
//        HDFS_Op.removeFile("/data/");
//        HDFS_Op.removeFile("/dataset/TIGER/");
        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/World/world/buildings_80M.csv", "/data/World");
        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/World/world/linestrings_72M.csv", "/data/World");
        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/World/world/points_80M.csv", "/data/World");
        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/World/world/points_80M_wkt.csv", "/data/World");
        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/World/world/rectangles_80M.csv", "/data/World");
//        HDFS_Op.upload("/home/runxuan/data/Spatial_Compare/points_10M_wkt.csv","/data");
//        HDFS_Op.upload("/home/runxuan/data/transform_trim/TIGER","/dataset/Tiger_trim");
    }

//    public static void main(String[] args) throws Exception {
////        System.setProperty("hadoop.home.dir", "D:\\hadoop-3.2.2");
//        Configuration conf = new Configuration();
////        conf.set("dfs.replication", "3");
//        FileSystem hdfs = FileSystem.get(new URI("hdfs://node1:9000"), conf, "root");
//
//        //读取本地文件
//        InputStream in = new FileInputStream("D:/1.pdf");
//        //在Hdfs上创建一个文件，返回输出流
//        OutputStream out = hdfs.create(new Path("/1.pdf"));
//        //输入 ---》  输出
//        IOUtils.copyBytes(in, out, 4096, true);
//
//    }


}
