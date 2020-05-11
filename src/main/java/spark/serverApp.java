package spark;

import java.io.File;
import java.util.LinkedHashMap;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ServerApp {
	
	public static void main(String args[]) {
		
		//runs server
		if (args[0].equalsIgnoreCase("server")) {
			startTomcat();
			
		} 
		
		// used to upload text file data and run server
		else if (args[0].equalsIgnoreCase("loadtext")) {
			String textFile = args[1];
			
			SparkConf conf = new SparkConf()
					.setAppName("Testing read in")
					.setMaster("local");

			JavaSparkContext sc = new JavaSparkContext(conf);
			
			//loadtext file
			JavaRDD<MyCSVFile> data = new LoadCSVFile(textFile).createRDD(sc);
			
			//spark manipulation
			SparkTransformations sparkTransformations = new SparkTransformations(data);
			
			System.out.println(sparkTransformations.getDataStorage("countGender"));
			
			System.out.println(sparkTransformations.getDataStorage("countRace"));
			
			System.out.println(sparkTransformations.getDataStorage("countParentEducation"));
			
			System.out.println(sparkTransformations.getDataStorage("countLunch"));
			
			System.out.println(sparkTransformations.getDataStorage("countTestPrep"));
			
			System.out.println("Math Score Avg is: " + sparkTransformations.getDataStorage("mathScoreAvg"));

			System.out.println("Reading Score Avg is: " + sparkTransformations.getDataStorage("readingScoreAvg"));

			System.out.println("Writing Score Avg is: " + sparkTransformations.getDataStorage("writingScoreAvg"));
			
			System.out.println("Overall Score Avg is: " + sparkTransformations.getDataStorage("overallScoreAvg"));
			
			System.out.println("Overall Gender Score Avg is: " + sparkTransformations.getDataStorage("genderAvg"));
			
			System.out.println("Overall Race Score Avg is: " + sparkTransformations.getDataStorage("raceAvg"));
			
			System.out.println("Overall ParentEducation Score Avg is: " + sparkTransformations.getDataStorage("parentEducationAvg"));
			
			System.out.println("Overall Lunch Score Avg is: " + sparkTransformations.getDataStorage("lunchAvg"));
			
			System.out.println("Overall TestPrep Score Avg is: " + sparkTransformations.getDataStorage("testPrepAvg"));
			
			
			//send to to database
			LinkedHashMap<String, String> dataStorage = sparkTransformations.getDataStorage();
			SQLRepo sqlRepo = new SQLRepo();
			
			for(String key : dataStorage.keySet()) {
				sqlRepo.insert(key,dataStorage.get(key));
			}

			//initiate embed tomcat
			startTomcat();
			
			//close JavaSparkContect
			sc.close();
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
