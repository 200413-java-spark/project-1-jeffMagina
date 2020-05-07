package spark;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.functions;

import scala.Tuple2;

@WebServlet(value = "/goodbye")
public class GoodbyeServlet extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		SparkConf conf = new SparkConf()
			.setAppName("Testing read in")
			.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> allRows = sc.textFile("C:\\Users\\Jeff\\Revature\\project-1\\dummyData.csv");
		
		String header = allRows.first();
		JavaRDD<String> filteredRows = allRows.filter(row -> !row.equals(header));
		
		JavaPairRDD<String, MyCSVFile> filteredRowPairRDD = filteredRows.mapToPair(parseCSVFile);
		filteredRowPairRDD.foreach(data -> {
			
			System.out.println(data._1() + " ### " + data._2().name + data._2().age + data._2().gender);
			

		});
		
		//counts how many of each gender
		JavaPairRDD<String, Integer> genders = filteredRowPairRDD.mapToPair((f) -> new Tuple2<>(f._2.gender,1));
		System.out.println(genders.collect());	
		JavaPairRDD<String, Integer> genderscount = genders.reduceByKey((x,y) -> ((int)x + (int)y));
		System.out.println(genderscount.collect());
		
		//avg the ages
		double avg = filteredRowPairRDD.map((f) -> f._2.age).reduce((x,y) -> ((int)x + (int)y))/filteredRowPairRDD.count();
		
		System.out.println(avg);

		sc.stop();
		sc.close();
	}
	
	private static PairFunction<String, String, MyCSVFile> parseCSVFile = (row) -> {
		String[] fields = row.split(",");
		return new Tuple2<String, MyCSVFile>(row, new MyCSVFile(fields[0],fields[1],fields[2]));
	};

}
