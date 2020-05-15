package com.github.jeffmagina.project1.spark;

import java.util.LinkedHashMap;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SparkTransformationsTest {
	
	LinkedHashMap<String,String> expectedData = new LinkedHashMap<String,String>();
	LinkedHashMap<String, String> actualData = new LinkedHashMap<String,String>();
	
	@Before
	public void setup() {
		//setup JavaSparkContext
	
		SparkTransformations sparkTransformations = new SparkTransformations("Test.csv");
		actualData = sparkTransformations.getDataStorage();
		
		//expectedData
		expectedData.put("CountGender", "[(female,3)]");
		expectedData.put("CountRace", "[(group B,2), (group C,1)]");
		expectedData.put("CountParentEducation", "[(bachelor's degree,1), (master's degree,1), (some college,1)]");
		expectedData.put("CountLunch", "[(standard,3)]");
		expectedData.put("CountTestPrep", "[(completed,1), (none,2)]");
		
		expectedData.put("MathAvgGender", "[(female,77.0)]");
		expectedData.put("ReadingAvgGender", "[(female,85.67)]");
		expectedData.put("WritingAvgGender", "[(female,85.0)]");
		expectedData.put("OverallAvgGender", "[(female,82.56)]");
		
		expectedData.put("MathAvgRace", "[(group B,81.0), (group C,69.0)]");
		expectedData.put("ReadingAvgRace", "[(group B,83.5), (group C,90.0)]");
		expectedData.put("WritingAvgRace", "[(group B,83.5), (group C,88.0)]");
		expectedData.put("OverallAvgRace", "[(group B,82.67), (group C,82.33)]");
		
		expectedData.put("MathAvgParentEducation", "[(bachelor's degree,72.0), (master's degree,90.0), (some college,69.0)]");
		expectedData.put("ReadingAvgParentEducation", "[(bachelor's degree,72.0), (master's degree,95.0), (some college,90.0)]");
		expectedData.put("WritingAvgParentEducation", "[(bachelor's degree,74.0), (master's degree,93.0), (some college,88.0)]");
		expectedData.put("OverallAvgParentEducation", "[(bachelor's degree,72.67), (master's degree,92.67), (some college,82.33)]");
		
		expectedData.put("MathAvgLunch", "[(standard,77.0)]");
		expectedData.put("ReadingAvgLunch", "[(standard,85.67)]");
		expectedData.put("WritingAvgLunch", "[(standard,85.0)]");
		expectedData.put("OverallAvgLunch", "[(standard,82.56)]");
		
		expectedData.put("MathAvgTestPrep", "[(completed,69.0), (none,81.0)]");
		expectedData.put("ReadingAvgTestPrep", "[(completed,90.0), (none,83.5)]");
		expectedData.put("WritingAvgTestPrep", "[(completed,88.0), (none,83.5)]");
		expectedData.put("OverallAvgTestPrep", "[(completed,82.33), (none,82.67)]");
		
		expectedData.put("CountOverall", "3.0");
		expectedData.put("MathAvgOverall", "77.0");
		expectedData.put("ReadingAvgOverall", "85.67");
		expectedData.put("WritingAvgOverall", "85.0");
		expectedData.put("OverallAvgOverall", "82.56");
		
	}
	
	//count avg test
	@Test
	public void whenCountingGender() {
		assertEquals(expectedData.get("CountGender"),actualData.get("CountGender"));
	}
	@Test 
	public void whenCountingRace() {
		assertEquals(expectedData.get("CountRace"),actualData.get("CountRace"));
	}
	@Test 
	public void whenCountingParentEducation() {
		assertEquals(expectedData.get("CountParentEducation"),actualData.get("CountParentEducation"));
	}
	@Test 
	public void whenCountingProvidedLunch() {
		assertEquals(expectedData.get("CountLunch"),actualData.get("CountLunch"));
	}
	@Test 
	public void whenCountingTestPrep() {
		assertEquals(expectedData.get("CountTestPrep"),actualData.get("CountTestPrep"));
	}

	//gender avg test
	@Test 
	public void whenAveragingMathScoresForGender() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingReadingScoresForGender() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingWritingScoresForGender() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingOverallScoresForGender() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	
	//race avg test
	@Test 
	public void whenAveragingMathScoresForRace() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingReadingScoresForRace() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingWritingScoresForRace() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingOverallScoresForRace() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	
	//parent education avg test
	@Test 
	public void whenAveragingMathScoresForParentEducation() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingReadingScoresForParentEducation() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingWritingScoresForParentEducation() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingOverallScoresForParentEducation() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	
	//provided lunch avg test
	@Test 
	public void whenAveragingMathScoresForLunch() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingReadingScoresForLunch() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingWritingScoresForLunch() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingOverallScoresForLunch() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	
	//test prep avg test
	@Test 
	public void whenAveragingMathScoresForTestPrep() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingReadingScoresForTestPrep() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingWritingScoresForTestPrep() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	@Test 
	public void whenAveragingOverallScoresForTestPrep() {
		assertEquals(expectedData.get("MathAvgGender"),actualData.get("MathAvgGender"));
	}
	
	//overall count/avg test
	@Test 
	public void whenCountingOverall() {
		assertEquals(expectedData.get("CountOverall"),actualData.get("CountOverall"));
	}
	@Test 
	public void whenAveragingMathScoresOverall() {
		assertEquals(expectedData.get("MathAvgOverall"),actualData.get("MathAvgOverall"));
	}
	@Test 
	public void whenAveragingReadingScoresOverall() {
		assertEquals(expectedData.get("ReadingAvgOverall"),actualData.get("ReadingAvgOverall"));
	}
	@Test 
	public void whenAveragingWritingScoresOverall() {
		assertEquals(expectedData.get("WritingAvgOverall"),actualData.get("WritingAvgOverall"));
	}
	@Test 
	public void whenAveragingOverallScoresOverall() {
		assertEquals(expectedData.get("OverallAvgOverall"),actualData.get("OverallAvgOverall"));
	}
}
