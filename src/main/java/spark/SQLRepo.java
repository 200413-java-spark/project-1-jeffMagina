package spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SQLRepo implements Dao{

	@Override
	public void insert(Object e) {
		
		try (Connection conn = SqlDataSource.getConnection();){
			
		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}
		
	}
	@Override
	public List readAll() {
		ArrayList<String> testArray = new ArrayList<>();
		try (Connection conn = SqlDataSource.getConnection();){
			Statement testStmt = conn.createStatement();
			ResultSet testRS = testStmt.executeQuery("Select * from customer");
			while (testRS.next()) {
				//grab database info and add to arraylist
			}
			
		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}
		return testArray;
	}
}
