package spark;

public class MyCSVFile {
	String name;
	int age;
	String gender;
	
	MyCSVFile(String name, String age, String gender){
		this.name = name;
		this.age = Integer.parseInt(age);
		this.gender = gender;
	}

}
