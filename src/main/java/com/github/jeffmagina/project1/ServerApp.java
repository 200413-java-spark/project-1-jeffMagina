package com.github.jeffmagina.project1;

import java.io.File;
import java.util.LinkedHashMap;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import com.github.jeffmagina.project1.io.SQLRepo;
import com.github.jeffmagina.project1.spark.SparkTransformations;
import com.github.jeffmagina.project1.web.JeffsProject1Servlet;

public class ServerApp {
	
	public static void main(String args[]) {
		
		// run server
		if (args[0].equalsIgnoreCase("server")) {
			startTomcat();
		} 
		
		// upload text file data and run server
		else if (args[0].equalsIgnoreCase("loadCSV")) {
			String textFile = args[1];
			
			//load TextFile -> create RDD -> do some Transformations using Spark
			SparkTransformations sparkTransformations = new SparkTransformations(textFile);
			
			//Create linked hash map to send to database
			LinkedHashMap<String, String> dataStorage = sparkTransformations.getDataStorage();

			//send to to database		
			SQLRepo sqlRepo = new SQLRepo();
			
			for(String key : dataStorage.keySet()) {
				sqlRepo.insert(key,dataStorage.get(key));
			}
	
			//initiate embed tomcat
			startTomcat();
		}
	}

	private static void startTomcat() {
		Tomcat tomcat = new Tomcat();
		tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
		tomcat.setPort(8080);
		tomcat.getConnector();
		tomcat.addWebapp("/spark", new File("src/main/resources/").getAbsolutePath());
		tomcat.addServlet("/spark", "JeffsProject1Servlet", new JeffsProject1Servlet()).addMapping("/JeffsProject1");
		try {
			tomcat.start();
		} catch (LifecycleException ex) {
			System.err.println(ex.getMessage());
		}
		
	}

}
