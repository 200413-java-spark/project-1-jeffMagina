package spark;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;


@WebServlet(value = "/hello")
public class HelloServlet extends HttpServlet {
	
	Dataset<Row> df ;
	
	
	
	@Override
	public void init() throws ServletException {
		//Read in text file into Dataset
		SparkSession sparkSession = SparkSession.builder()
				.appName("Test parser of csv")
				.master("local")
				.getOrCreate();
		
		StructType schema = new StructType()
				.add("Name","string")
				.add("Age","integer")
				.add("Gender","string");
			
			df = sparkSession.read()
					.schema(schema)
					.option("header",true)
					.option("mode","DROPMALFORMED")
					.csv("C:\\Users\\Jeff\\Revature\\project-1\\dummyData.csv");
			
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		resp.getWriter().println(df.collectAsList());		
		
		df.printSchema();
		df.select("Name").show();
		df.select("Age").show();
		df.select("Gender").show();
		//df.agg(functions.count(functions.when(df.col("Gender").equalTo("Male"), 1))).collect());
		
		
		resp.getWriter().println("count= " + df.agg(functions.count(functions.when(df.col("Gender").equalTo("Male"), 1))).collectAsList().get(0).toString());
		
		
		//persist data to database
		

	}

}
