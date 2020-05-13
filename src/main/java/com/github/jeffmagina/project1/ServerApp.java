package com.github.jeffmagina.project1;

import java.io.File;
import java.util.LinkedHashMap;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.github.jeffmagina.project1.io.LoadCSVFile;
import com.github.jeffmagina.project1.io.MyCSVFile;
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
			
			SparkConf conf = new SparkConf()
					.setAppName("Testing read in")
					.setMaster("local");

			JavaSparkContext sc = new JavaSparkContext(conf);
			
			//loadtext file
			JavaRDD<MyCSVFile> data = new LoadCSVFile(textFile).createRDD(sc);
			
			//send to to database			
			SparkTransformations sparkTransformations = new SparkTransformations(data);
			LinkedHashMap<String, String> dataStorage = sparkTransformations.getDataStorage();
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
