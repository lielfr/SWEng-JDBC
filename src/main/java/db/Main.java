package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.net.ssl.SSLException;
import javax.xml.transform.Result;

public class Main {
	static private final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

	// update USER, PASS and DB URL according to credentials provided by the website:
	// https://remotemysql.com/
	// in future move these hard coded strings into separated config file or even better env variables
	static private final String DB = "5SBSrSod8p";
	static private final String DB_URL = "jdbc:mysql://remotemysql.com/"+ DB + "?useSSL=false";
	static private final String USER = "5SBSrSod8p";
	static private final String PASS = "vHRge7mGoV";

	private static void outputDB(Statement stmt) throws SQLException {
		String sql = "SELECT * FROM flights";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			int num = rs.getInt("num");
			String origin = rs.getString("origin");
			String destination = rs.getString("destination");
			int distance = rs.getInt("distance");
			int price = rs.getInt("price");

			System.out.format("Number %5s Origin %15s destinations %18s Distance %5d Price %5d\n", num, origin, destination, distance, price);
		}
		rs.close();
	}

	public static void main(String[] args) throws SSLException {
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			// Queries are here
			PreparedStatement update1 = conn.prepareStatement("UPDATE flights SET price = ? WHERE num = ?");
			update1.setInt(1, 2019);
			update1.setInt(2, 387);
			update1.executeUpdate();
			update1.close();

			ResultSet afterUpdate = stmt.executeQuery("SELECT * FROM flights WHERE num = 387");
			while (afterUpdate.next()) {
				int num = afterUpdate.getInt("num");
				int price = afterUpdate.getInt("price");

				System.out.format("Updated: the price of flight no %d is %d\n", num, price);
			}

			afterUpdate.close();

			stmt.executeUpdate("UPDATE flights SET price = price + 100 WHERE distance > 1000");
			ResultSet batchUpdate = stmt.executeQuery("SELECT * from flights WHERE price < 300");
			while (batchUpdate.next()) {
				int price = batchUpdate.getInt("price");
				price -= 25;
				batchUpdate.updateInt("price", 25);
				batchUpdate.updateRow();
			}
			batchUpdate.close();

			System.out.println("After C:");
			outputDB(stmt);

			PreparedStatement update2 = conn.prepareStatement("UPDATE flights SET price = price + ? WHERE distance > ?");
			update2.setInt(1, 100);
			update2.setInt(2, 1000);
			update2.executeUpdate();
			update2.close();

			PreparedStatement update3 = conn.prepareStatement("UPDATE flights SET price = price - ? WHERE price < ?");
			update3.setInt(1, 25);
			update3.setInt(2, 300);
			update3.executeUpdate();
			update3.close();

			System.out.println("After D:");
			outputDB(stmt);


			System.out.println("\t============");


			outputDB(stmt);

			String sql;
			ResultSet rs;


			System.out.println("\t============");

			sql = "SELECT origin, destination, distance, num FROM flights";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String origin = rs.getString("origin");
				String destination = rs.getString("destination");
				int distance = rs.getInt("distance");

				System.out.print("From: " + origin);
				System.out.print(",\tTo: " + destination);
				System.out.println(",\t\tDistance: " + distance);
			}

			System.out.println("\t============");
			
			sql = "SELECT origin, destination FROM flights WHERE distance > ?";
			PreparedStatement prep_stmt = conn.prepareStatement(sql);
			prep_stmt.setInt(1, 200);
			rs = prep_stmt.executeQuery();
			while (rs.next()) {
				String origin = rs.getString("origin");
				System.out.println("From: " + origin);
			}
			
			rs.close();
			stmt.close();
			conn.close();

		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("SQLException: " + se.getMessage());
            System.out.println("SQLState: " + se.getSQLState());
            System.out.println("VendorError: " + se.getErrorCode());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
	}
}
