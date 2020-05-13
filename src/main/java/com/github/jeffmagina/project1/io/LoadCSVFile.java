package com.github.jeffmagina.project1.io;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LoadCSVFile {
	private String textFile;

	public LoadCSVFile(String textFile) {
		this.textFile = textFile;
	}

	public JavaRDD<MyCSVFile> createRDD(JavaSparkContext sc) {

		JavaRDD<String> allRows = sc.textFile(new File(textFile).getAbsolutePath()).cache();
		
		//filter out headers
		String header = allRows.first();
		JavaRDD<String> headerlessRows = allRows.filter(row -> !row.equals(header)).cache();
				
		// map to MyCSVFile data struct
		// MyCSVFile contains gender, race, parentEducation, lunch, testPrep, mathScore, readingScore, writingScore
		JavaRDD<MyCSVFile> filteredHeaderlessRow = headerlessRows.map( (n) -> {
			String[] fields = n.split(",");
			return new MyCSVFile(fields[0],fields[1],fields[2], fields[3], fields[4], fields[5], fields[6], fields[7]);
		}).cache();
		
		return filteredHeaderlessRow;
	}
}