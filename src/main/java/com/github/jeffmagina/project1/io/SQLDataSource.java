package com.github.jeffmagina.project1.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLDataSource {

	private static String url;
	private static String user;
	private static String password;

	static {
		url = System.getProperty("database.url", "jdbc:postgresql://18.189.28.79:5433/jeffMaginaProject1");
		user = System.getProperty("database.username", "jeffMagina");
		password = System.getProperty("database.password", "jeffMagina");
	}

	private SQLDataSource() {
	}

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}

}
