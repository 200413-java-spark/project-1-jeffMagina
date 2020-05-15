package com.github.jeffmagina.project1.spark;

public class MyCSVFile {
	public String gender;
	public String race;
	public String parentEducation;
	public String lunch;
	public String testPrep;
	public int mathScore;
	public int readingScore;
	public int writingScore;

	public MyCSVFile(String gender, String race, String parentEducation, String lunch, String testPrep, String mathScore, String readingScore, String writingScore) {
		this.gender = gender;
		this.race = race;
		this.parentEducation = parentEducation;
		this.lunch = lunch;
		this.testPrep = testPrep;
		this.mathScore = Integer.parseInt(mathScore);
		this.readingScore = Integer.parseInt(readingScore);
		this.writingScore = Integer.parseInt(writingScore);
	}

}
