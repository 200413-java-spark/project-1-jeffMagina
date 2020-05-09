package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class serverApp {
	
	public static void main(String args[]) {
		
		if (args[0].equalsIgnoreCase("server")) {
			
			
		} else if (args[0].equalsIgnoreCase("loadtext")) {
			String textFile = args[1];
			
			SparkConf conf = new SparkConf()
					.setAppName("Testing read in")
					.setMaster("local");

			JavaSparkContext sc = new JavaSparkContext(conf);
			
			//loadtext file
			JavaRDD<MyCSVFile> data = new LoadCSVFile(textFile).createRDD(sc);
			
			//spark manipulation
			SparkTransformation sparkTransformation = new SparkTransformation(data);
			
			System.out.println(sparkTransformation.countGender().collect());
			
			System.out.println(sparkTransformation.countRace().collect());
			
			System.out.println(sparkTransformation.countParentEducation().collect());
			
			System.out.println(sparkTransformation.countLunch().collect());
			
			System.out.println(sparkTransformation.countTestPrep().collect());
			
			System.out.println("Math Score Avg is: " + sparkTransformation.mathScoreAvg());

			System.out.println("Reading Score Avg is: " + sparkTransformation.readingScoreAvg());

			System.out.println("Writing Score Avg is: " + sparkTransformation.writingScoreAvg());
			
			System.out.println("Overall Score Avg is: " + sparkTransformation.overallScoreAvg());
			
			System.out.println("Overall Gender Score Avg is: " + sparkTransformation.genderAvg().collect());
			
			System.out.println("Overall Race Score Avg is: " + sparkTransformation.raceAvg().collect());
			
			System.out.println("Overall ParentEducation Score Avg is: " + sparkTransformation.parentEducationAvg().collect());
			
			System.out.println("Overall Lunch Score Avg is: " + sparkTransformation.lunchAvg().collect());
			
			System.out.println("Overall TestPrep Score Avg is: " + sparkTransformation.testPrepAvg().collect());
			
			//send to to database
			//new persistToDatabase().insert(e);
			
			//tomcat pulls from database
			//call tomcat
			//close JavaSparkContect
			//sc.close();
		}
	}

}
