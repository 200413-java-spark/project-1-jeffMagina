package spark;

public class MyCSVFile {
	String gender;
	String race;
	String parentEducation;
	String lunch;
	String testPrep;
	int mathScore;
	int readingScore;
	int writingScore;

	MyCSVFile(String gender, String race, String parentEducation, String lunch, String testPrep, String mathScore, String readingScore, String writingScore) {
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
