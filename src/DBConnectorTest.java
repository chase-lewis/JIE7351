import java.sql.*;

public class DBConnectorTest {
	//private String ip = "128.61.18.34";
	private String connectionUrl[] = {"jdbc:sqlserver://", ":1433;DatabaseName=NBS_ODSE;user=sa;password=Nedss$GTRI;"};
	private ResultSet results;
	private Connection connection;

	public DBConnectorTest(String ip) {
		// Load SQL Server JDBC driver and establish connection.
		try{
			String url = connectionUrl[0] + ip + connectionUrl[1];
			System.out.print("Connecting to SQL Server url " + ip + " ... ");
			connection = DriverManager.getConnection(url);
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