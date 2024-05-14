package com.hadoop.hive;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS2 {

	public static void writeLocalFileToHDFS(String localFilePath, String hdfsFilePath) throws IOException {

		String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://hadoop-master:9000");

		try {
			FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

			// Path objects for local and HDFS files
			Path localPath = new Path(localFilePath);
			Path hdfsPath = new Path(hdfsFilePath);

			// Copy local file to HDFS
			fs.copyFromLocalFile(localPath, hdfsPath);

			System.out.println("Local file uploaded successfully to HDFS: " + hdfsPath.toString());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Optional: Close resources explicitly if not explicitly closed within the try
			// block
		}
	}
	
	public static void main(String[] args) {
		WriteHDFS2 hdfs2 = new WriteHDFS2();
		String localFilePath = "../hadoopLogHive/src/main/resources/user.log";
		String hdfsFilePath = "../kafka-hadoop/user.json";
		try {
			hdfs2.writeLocalFileToHDFS(localFilePath, hdfsFilePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
