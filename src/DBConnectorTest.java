import java.sql.*;

public class DBConnectorTest {
	private String connectionUrl = "jdbc:sqlserver://128.61.28.226:1433;DatabaseName=NBS_ODSE;user=test;password=test;";
	private ResultSet results;
	private Connection connection;

	public DBConnectorTest() {
		// Load SQL Server JDBC driver and establish connection.
		try{
			System.out.print("Connecting to SQL Server ... ");
			connection = DriverManager.getConnection(connectionUrl);
			System.out.println("Connected.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void query(String queryString) {
		try {
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