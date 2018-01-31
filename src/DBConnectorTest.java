import java.sql.*;

public class DBConnectorTest {
	private String connectionUrl = "jdbc:sqlserver://128.61.28.226:1433;DatabaseName=NBS_ODSE;user=test;password=test;";

	public DBConnectorTest() {
		System.out.println("We're Working!");
	}

	public ResultSet query(String queryString) {
		try {
			// Load SQL Server JDBC driver and establish connection.
			System.out.print("Connecting to SQL Server ... ");
			try (Connection connection = DriverManager.getConnection(connectionUrl)) {
				System.out.println("Done.");

				try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(queryString)) {
					System.out.println(resultSet);
					return resultSet;
				}
			}
		} catch (Exception e) {
			System.out.println();
			e.printStackTrace();
		}
	}
}