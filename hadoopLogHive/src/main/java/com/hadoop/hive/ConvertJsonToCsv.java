package com.hadoop.hive;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@SuppressWarnings({ "deprecation", "unused" })
public class ConvertJsonToCsv {
	
	 public static void main(String[] args) throws Exception {

	        // Replace with your actual file paths
	        String inputFile = "../hadoopLogHive/src/main/resources/log5.json";
	        String outputFile = "../hadoopLogHive/src/main/resources/log5.csv";

	        // Read JSON file
	        FileReader reader = new FileReader(inputFile);
	        JSONParser parser = new JSONParser();
	        Object obj = parser.parse(reader);

	        // Check for valid JSON object or array
	        if (obj instanceof JSONObject || obj instanceof JSONArray) {
	            convertToJson(obj, outputFile);
	        } else {
	            System.out.println("Error: Invalid JSON file format.");
	        }

	        reader.close();
	    }

	    private static void convertToJson(Object obj, String outputFile) throws Exception {

	        StringBuilder csvContent = new StringBuilder();

	        // Handle JSON Object
	        if (obj instanceof JSONObject) {
	            JSONObject jsonObject = (JSONObject) obj;
	            csvContent.append(extractHeaders(jsonObject)).append("\n");
	            csvContent.append(extractRow(jsonObject)).append("\n");
	        }

	        // Handle JSON Array
	        if (obj instanceof JSONArray) {
	            JSONArray jsonArray = (JSONArray) obj;
	            if (jsonArray.isEmpty()) {
	                System.out.println("Warning: Empty JSON array.");
	                return;
	            }
	            JSONObject firstObject = (JSONObject) jsonArray.get(0);
	            csvContent.append(extractHeaders(firstObject)).append("\n");
	            for (Object item : jsonArray) {
	                csvContent.append(extractRow((JSONObject) item)).append("\n");
	            }
	        }

	        // Write CSV content to file
	        FileWriter writer = new FileWriter(outputFile);
	        writer.write(csvContent.toString());
	        writer.close();

	        System.out.println("CSV file created successfully!");
	    }

	    private static String extractHeaders(JSONObject jsonObject) {
	        StringBuilder headers = new StringBuilder();
	        Iterator<String> iterator = jsonObject.keySet().iterator();
	        while (iterator.hasNext()) {
	            headers.append(iterator.next()).append(",");
	        }
	        return headers.substring(0, headers.length() - 1);
	    }

	    private static String extractRow(JSONObject jsonObject) {
	        StringBuilder row = new StringBuilder();
	        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
	        while (iterator.hasNext()) {
	            Map.Entry<String, Object> entry = iterator.next();
	            row.append(entry.getValue()).append(",");
	        }
	        return row.substring(0, row.length() - 1);
	    }
}
