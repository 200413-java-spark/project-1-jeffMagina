package spark;

import java.io.IOException;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class JeffsProject1Servlet extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		//retrieve data from database
		SQLRepo sqlRepo = new SQLRepo();
		sqlRepo.readAll();
		
		LinkedHashMap<String,String> databaseData = sqlRepo.readAll();

		for(String key : databaseData.keySet()) {
			resp.getWriter().println(databaseData.get(key));
		}

	}

}
