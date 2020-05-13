package com.github.jeffmagina.project1.spark;

import java.util.LinkedHashMap;

import org.apache.spark.api.java.JavaRDD;

import com.github.jeffmagina.project1.io.MyCSVFile;

import scala.Tuple2;

public class SparkTransformations {
	JavaRDD<MyCSVFile> data;
	private LinkedHashMap<String,String> dataStorage = new LinkedHashMap<String,String>();

	public SparkTransformations(JavaRDD<MyCSVFile> data) {
		this.data = data;
		this.dataStorage.put("countGender", countGender());
		this.dataStorage.put("countRace", countRace());
		this.dataStorage.put("countParentEducation", countParentEducation());
		this.dataStorage.put("countLunch", countLunch());
		this.dataStorage.put("countTestPrep", countTestPrep());
		this.dataStorage.put("mathScoreAvg", Double.toString(mathScoreAvg()));
		this.dataStorage.put("readingScoreAvg", Double.toString(readingScoreAvg()));
		this.dataStorage.put("writingScoreAvg", Double.toString(writingScoreAvg()));
		this.dataStorage.put("overallScoreAvg", Double.toString(overallScoreAvg()));
		this.dataStorage.put("genderAvg", genderAvg());
		this.dataStorage.put("raceAvg", raceAvg());
		this.dataStorage.put("parentEducationAvg", parentEducationAvg());
		this.dataStorage.put("lunchAvg", lunchAvg());
		this.dataStorage.put("testPrepAvg", testPrepAvg());

	}
	
	public String getDataStorage(String name) {
		return dataStorage.get(name);
	}
	
	public LinkedHashMap<String, String> getDataStorage() {
		return dataStorage;
	}

	private String countGender() {
		return data.mapToPair((f) -> new Tuple2<>(f.gender, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}

	private String countRace() {
		return data.mapToPair((f) -> new Tuple2<>(f.race, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}
	
	private String countParentEducation() {
		return data.mapToPair((f) -> new Tuple2<>(f.parentEducation, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}
	
	private String countLunch() {
		return data.mapToPair((f) -> new Tuple2<>(f.lunch, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}
	
	private String countTestPrep() {
		return data.mapToPair((f) -> new Tuple2<>(f.testPrep, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}
	
	private double mathScoreAvg(){
		return data.map((f) -> (double)f.mathScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	private double readingScoreAvg(){
		return  data.map((f) -> (double)f.readingScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	private double writingScoreAvg(){
		return  data.map((f) -> (double)f.writingScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	private double overallScoreAvg(){
		return (mathScoreAvg() + readingScoreAvg() + writingScoreAvg())/3;
	}
	
	private String genderAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.gender, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.collect()
				.toString();
		}	
	
	private String raceAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.race, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.collect()
				.toString();
	}
	
	private String parentEducationAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.parentEducation, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.collect()
				.toString();
	}
	
	private String lunchAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.lunch, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.collect()
				.toString();
	}
	
	private String testPrepAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.testPrep, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.collect()
				.toString();
	}
}
