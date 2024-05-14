package com.hadoop.hive;

import java.io.BufferedOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {

	public void writeForHDFS(String message) {
	    
	    String hdfsUri = "hdfs://hadoop-master:9000"; // HDFS URI
	    Configuration conf = new Configuration();
	    conf.set("fs.default.name", "hdfs://hadoop-master:9000");

	    try {
	        String filePath = "/path/to/your/hdfs/file/test.log"; // Replace with desired HDFS path
	        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

	        // Path object representing the file in HDFS
	        Path hdfsPath = new Path(filePath);

	        // Use create(overwrite) for directly writing new data
	        FSDataOutputStream os = fs.create(hdfsPath, true); // Set overwrite to true

	        // BufferedOutputStream for better performance
	        BufferedOutputStream bos = new BufferedOutputStream(os);
	        String data = message;

	        // Write data to the file
	        bos.write(data.getBytes());

	        bos.close();
	        os.close();

	        System.out.println("Data has been written to HDFS successfully.");

	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	        // Optional: Close resources explicitly if not closed within the try block
	    }
	}
}
