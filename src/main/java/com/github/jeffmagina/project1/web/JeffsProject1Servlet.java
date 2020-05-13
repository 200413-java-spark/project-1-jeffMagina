package com.github.jeffmagina.project1.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.jeffmagina.project1.io.SQLRepo;

public class JeffsProject1Servlet extends HttpServlet {

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String[] tokens = req.getParameterValues("token");
		String[] operations = req.getParameterValues("operation");

		PrintWriter out = resp.getWriter();
		resp.setContentType("text/html");

		out.print("<html><body>");
		out.print("<h1> Jeff's Project 1</h1>");
		
		//check to see if user input is incorrect
		if (tokens != null && operations != null) {
			if (tokens.length == 1) {
				out.print("<h2> Your Token Selection is:</h2>");
			} else {
				out.print("<h2> Your Tokens are:</h2>");
			}
			out.print("<ul>");
			for (String s : tokens) {
				out.print("<li>" + s + "</li>");
			}
			out.print("</ul>");

			if (operations.length == 1) {
				out.print("<h2> Your Operation Selection is:</h2>");
			} else {
				out.print("<h2> Your Operation Selections are:</h2>");
			}

			out.print("<ul>");

			for (String s : operations) {
				out.print("<li>" + s + "</li>");
			}
			out.print("</ul>");

			// read in database into a linkedhash map
			SQLRepo sqlRepo = new SQLRepo();
			sqlRepo.readAll();
			LinkedHashMap<String, String> databaseData = sqlRepo.readAll();

			//print results based on specified user input
			out.print("<h3>");
			for (int i = 0; tokens.length > i; i++) {
				out.print("<h4>" + tokens[i] + "</h4>");
				out.print("<ul>");
				for (int j = 0; operations.length > j; j++) {
					out.println("<li>" + tokens[i] + " + " + operations[j] + " Results are: "
							+ databaseData.get(operations[j] + tokens[i]) + "</li>");
				}
				out.print("</ul>");
			}
			out.print("</h3>");
			out.print("</html></body>");

			// button to go back
			out.print("<button type=\"button\" name=\"back\" onclick=\"history.back()\">back</button>");
		} else {
			out.print("<h2>Select at least one item from each chechbox list!</h2>");
			out.print("<h3>You will be redirected back in 5 seconds</h3>");
			resp.addHeader("REFRESH", "5,URL=data.html");
		}
	}
}
