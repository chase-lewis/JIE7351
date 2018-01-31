import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseQuery {

	public static void main(String[] args) {

		String connectionUrl = "jdbc:sqlserver://128.61.24.3:1433;DatabaseName=NBS_ODSE;user=test;password=test;";

		try {
			// Load SQL Server JDBC driver and establish connection.
			System.out.print("Connecting to SQL Server ... ");
			try (Connection connection = DriverManager.getConnection(connectionUrl)) {
				System.out.println("Done.");
			}
		} catch (Exception e) {
			System.out.println();
			e.printStackTrace();
		}
	}
}