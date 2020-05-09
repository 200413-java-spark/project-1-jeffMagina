package spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class SparkTransformation {
	JavaRDD<MyCSVFile> data;

	SparkTransformation(JavaRDD<MyCSVFile> data) {
		this.data = data;
	}

	public JavaPairRDD<String, Integer> countGender() {
		return data.mapToPair((f) -> new Tuple2<>(f.gender, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
	}

	public JavaPairRDD<String, Integer> countRace() {
		return data.mapToPair((f) -> new Tuple2<>(f.race, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
	}
	
	public JavaPairRDD<String, Integer> countParentEducation() {
		return data.mapToPair((f) -> new Tuple2<>(f.parentEducation, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
	}
	
	public JavaPairRDD<String, Integer> countLunch() {
		return data.mapToPair((f) -> new Tuple2<>(f.lunch, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
	}
	
	public JavaPairRDD<String, Integer> countTestPrep() {
		return data.mapToPair((f) -> new Tuple2<>(f.testPrep, 1)).reduceByKey((x, y) -> ((int) x + (int) y));
	}
	
	public double mathScoreAvg(){
		return data.map((f) -> (double)f.mathScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	public double readingScoreAvg(){
		return data.map((f) -> (double)f.readingScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	public double writingScoreAvg(){
		return data.map((f) -> (double)f.writingScore).reduce((x,y) -> (x + y))/data.count();
	}
	
	public double overallScoreAvg(){
		return (mathScoreAvg() + readingScoreAvg() + writingScoreAvg())/3;
	}
	
	public JavaPairRDD<String, Double> genderAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.gender, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2);		
		}	
	
	public JavaPairRDD<String, Double> raceAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.race, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2);
	}
	
	public JavaPairRDD<String, Double> parentEducationAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.parentEducation, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2);
	}
	
	public JavaPairRDD<String, Double> lunchAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.lunch, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2);
	}
	
	public JavaPairRDD<String, Double> testPrepAvg() {
		return data.mapToPair(f -> new Tuple2<>(f.testPrep, (((double)f.mathScore + (double)f.readingScore + (double)f.writingScore)/3)))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2);
	}
}
