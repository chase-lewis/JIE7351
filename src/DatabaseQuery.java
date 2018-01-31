import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DatabaseQuery {

	public static void main(String[] args) {

		String connectionUrl = "jdbc:sqlserver://128.61.28.226:1433;DatabaseName=NBS_ODSE;user=test;password=test;";

		try {
			// Load SQL Server JDBC driver and establish connection.
			System.out.print("Connecting to SQL Server ... ");
			try (Connection connection = DriverManager.getConnection(connectionUrl)) {
				System.out.println("Done.");

				String sql = "UPDATE NBS_ODSE.dbo.Person SET add_reason_cd='YES IT WORKS 2', add_time=GETDATE() WHERE person_uid='10000001'";
				try (Statement statement = connection.createStatement()) {
					statement.executeUpdate(sql);
					System.out.println("Done. ");
				}
			}
		} catch (Exception e) {
			System.out.println();
			e.printStackTrace();
		}
	}
}