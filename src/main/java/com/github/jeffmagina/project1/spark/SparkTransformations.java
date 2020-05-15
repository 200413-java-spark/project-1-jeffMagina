package com.github.jeffmagina.project1.spark;

import java.io.File;
import java.util.LinkedHashMap;

import org.apache.commons.math3.util.Precision;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkTransformations {
	JavaRDD<MyCSVFile> data;
	private LinkedHashMap<String, String> dataStorage = new LinkedHashMap<String, String>();

	JavaSparkContext sc;

	public SparkTransformations(String textFile) {

		SparkConf conf = new SparkConf().setAppName("ServerApp").setMaster("local");

		sc = new JavaSparkContext(conf);

		this.data = createRDD(textFile);
		// count spark manipulations
		this.dataStorage.put("CountGender", countGender());
		this.dataStorage.put("CountRace", countRace());
		this.dataStorage.put("CountParentEducation", countParentEducation());
		this.dataStorage.put("CountLunch", countLunch());
		this.dataStorage.put("CountTestPrep", countTestPrep());

		// gender avg spark manipulations
		this.dataStorage.put("MathAvgGender", genderMathAvg());
		this.dataStorage.put("ReadingAvgGender", genderReadingAvg());
		this.dataStorage.put("WritingAvgGender", genderWritingAvg());
		this.dataStorage.put("OverallAvgGender", genderOverallAvg());

		// race avg spark manipulations
		this.dataStorage.put("MathAvgRace", raceMathAvg());
		this.dataStorage.put("ReadingAvgRace", raceReadingAvg());
		this.dataStorage.put("WritingAvgRace", raceWritingAvg());
		this.dataStorage.put("OverallAvgRace", raceOverallAvg());

		// parent education avg spark manipulations
		this.dataStorage.put("MathAvgParentEducation", parentEducationMathAvg());
		this.dataStorage.put("ReadingAvgParentEducation", parentEducationReadingAvg());
		this.dataStorage.put("WritingAvgParentEducation", parentEducationWritingAvg());
		this.dataStorage.put("OverallAvgParentEducation", parentEducationOverallAvg());

		// provided lunch avg spark manipulations
		this.dataStorage.put("MathAvgLunch", lunchMathAvg());
		this.dataStorage.put("ReadingAvgLunch", lunchReadingAvg());
		this.dataStorage.put("WritingAvgLunch", lunchWritingAvg());
		this.dataStorage.put("OverallAvgLunch", lunchOverallAvg());

		// test prep avg spark manipulations
		this.dataStorage.put("MathAvgTestPrep", testPrepMathAvg());
		this.dataStorage.put("ReadingAvgTestPrep", testPrepReadingAvg());
		this.dataStorage.put("WritingAvgTestPrep", testPrepWritingAvg());
		this.dataStorage.put("OverallAvgTestPrep", testPrepOverallAvg());

		// overall counts/avgs
		this.dataStorage.put("CountOverall", Double.toString(overallCount()));
		this.dataStorage.put("MathAvgOverall", Double.toString(mathScoreAvg()));
		this.dataStorage.put("ReadingAvgOverall", Double.toString(readingScoreAvg()));
		this.dataStorage.put("WritingAvgOverall", Double.toString(writingScoreAvg()));
		this.dataStorage.put("OverallAvgOverall", Double.toString(overallScoreAvg()));

		sc.close();

	}

	public JavaRDD<MyCSVFile> createRDD(String textFile) {
		
		//read in raw textfile to an rdd
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

	public String getDataStorage(String name) {
		return dataStorage.get(name);
	}

	public LinkedHashMap<String, String> getDataStorage() {
		return dataStorage;
	}

	// count
	private String countGender() {
		return data.mapToPair((f) -> new Tuple2<>(f.gender, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect()
				.toString();
	}

	private String countRace() {
		return data.mapToPair((f) -> new Tuple2<>(f.race, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect()
				.toString();
	}

	private String countParentEducation() {
		return data.mapToPair((f) -> new Tuple2<>(f.parentEducation, 1)).reduceByKey((x, y) -> ((int) x + (int) y))
				.collect().toString();
	}

	private String countLunch() {
		return data.mapToPair((f) -> new Tuple2<>(f.lunch, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect()
				.toString();
	}

	private String countTestPrep() {
		return data.mapToPair((f) -> new Tuple2<>(f.testPrep, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect()
				.toString();
	}

	// gender
	private String genderMathAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.gender, (double) f.mathScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String genderReadingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.gender, (double) f.readingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String genderWritingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.gender, (double) f.writingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String genderOverallAvg() {
		return data
				.mapToPair(f -> new Tuple2<>(f.gender,
						(((double) f.mathScore + (double) f.readingScore + (double) f.writingScore) / 3)))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	// race
	private String raceMathAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.race, (double) f.mathScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String raceWritingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.race, (double) f.writingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String raceReadingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.race, (double) f.readingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String raceOverallAvg() {
		return data
				.mapToPair(f -> new Tuple2<>(f.race,
						(((double) f.mathScore + (double) f.readingScore + (double) f.writingScore) / 3)))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	// parent education
	private String parentEducationMathAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.parentEducation, (double) f.mathScore))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String parentEducationWritingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.parentEducation, (double) f.writingScore))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String parentEducationReadingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.parentEducation, (double) f.readingScore))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String parentEducationOverallAvg() {
		return data
				.mapToPair(f -> new Tuple2<>(f.parentEducation,
						(((double) f.mathScore + (double) f.readingScore + (double) f.writingScore) / 3)))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	// provided lunch
	private String lunchMathAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.lunch, (double) f.mathScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String lunchWritingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.lunch, (double) f.writingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String lunchReadingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.lunch, (double) f.readingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String lunchOverallAvg() {
		return data
				.mapToPair(f -> new Tuple2<>(f.lunch,
						(((double) f.mathScore + (double) f.readingScore + (double) f.writingScore) / 3)))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	// test prep
	private String testPrepMathAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.testPrep, (double) f.mathScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String testPrepWritingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.testPrep, (double) f.writingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String testPrepReadingAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.testPrep, (double) f.readingScore)).mapValues(f -> new Tuple2<>(f, 1))
				.reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	private String testPrepOverallAvg() {
		return data
				.mapToPair(f -> new Tuple2<>(f.testPrep,
						(((double) f.mathScore + (double) f.readingScore + (double) f.writingScore) / 3)))
				.mapValues(f -> new Tuple2<>(f, 1)).reduceByKey((x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> Precision.round(f._1 / f._2, 2)).collect().toString();
	}

	// overall
	private double overallCount() {
		return data.count();
	}

	private double mathScoreAvg() {
		return Precision.round(data.map((f) -> (double) f.mathScore).reduce((x, y) -> (x + y)) / data.count(), 2);
	}

	private double readingScoreAvg() {
		return Precision.round(data.map((f) -> (double) f.readingScore).reduce((x, y) -> (x + y)) / data.count(), 2);
	}

	private double writingScoreAvg() {
		return Precision.round(data.map((f) -> (double) f.writingScore).reduce((x, y) -> (x + y)) / data.count(), 2);
	}

	private double overallScoreAvg() {
		return Precision.round((mathScoreAvg() + readingScoreAvg() + writingScoreAvg()) / 3, 2);
	}
}
