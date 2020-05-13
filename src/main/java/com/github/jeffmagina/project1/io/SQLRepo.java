package com.github.jeffmagina.project1.io;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;


public class SQLRepo {

	public void insert(String key, String value) {
		
		try (Connection conn = SQLDataSource.getConnection();){
			// insert into SparkTransformations table, id auto generated
			PreparedStatement SparkTransStmt = conn.prepareStatement("insert into SparkTransformations(name,transformation) values (?,?)");
			SparkTransStmt.setString(1, key);
			SparkTransStmt.setString(2, value);
			SparkTransStmt.addBatch();
			SparkTransStmt.executeBatch();
			
		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}
	}
	
	public LinkedHashMap<String, String> readAll() {
		LinkedHashMap<String, String> dataStorage = new LinkedHashMap<String, String>();
		try (Connection conn = SQLDataSource.getConnection();){
			Statement SparkTransStmt = conn.createStatement();
			ResultSet SparkTransStmtRs = SparkTransStmt.executeQuery("Select * from SparkTransformations");
			while (SparkTransStmtRs.next()) {
				dataStorage.put(SparkTransStmtRs.getString("name"), SparkTransStmtRs.getString("transformation"));
			}		
		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}
		return dataStorage;
	}
}
