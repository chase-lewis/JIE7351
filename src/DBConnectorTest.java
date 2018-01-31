import java.sql.*;

public class DBConnectorTest {
	private String connectionUrl = "jdbc:sqlserver://128.61.28.226:1433;DatabaseName=NBS_ODSE;user=test;password=test;";
	private ResultSet results;

	public DBConnectorTest() {
		System.out.println("We're Working!");
	}

	public void query(String queryString) {
		try {
			// Load SQL Server JDBC driver and establish connection.
			System.out.print("Connecting to SQL Server ... ");
			Connection connection = DriverManager.getConnection(connectionUrl);
			System.out.println("Connected.");
			Statement statement = connection.createStatement();
			results = statement.executeQuery(queryString);
				// System.out.println(results);
		} catch (Exception e) {
			System.out.println();
			e.printStackTrace();
		}
	}

	public ResultSet getResults() {
		return results;
	}
}