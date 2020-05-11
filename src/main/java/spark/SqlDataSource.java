package spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLDataSource {

	private static String url;
	private static String user;
	private static String password;

	static {
		url = System.getProperty("database.url", "jdbc:postgresql://52.14.154.73:5433/jeffMaginaProject1");
		user = System.getProperty("database.username", "jeffMagina");
		password = System.getProperty("database.password", "jeffMagina");
	}

	private SQLDataSource() {
	}

	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, user, password);
	}

}
