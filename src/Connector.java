import java.sql.*;

public class Connector {
	//private String ip = "128.61.18.34";
	private String connectionUrl[] = {"jdbc:sqlserver://", ":1433;user=test;password=test;"};
	private ResultSet results;
	private Connection connection;

	public Connector(String ip) {
		// Load SQL Server JDBC driver and establish connection.
		try{
			String url = connectionUrl[0] + ip + connectionUrl[1];
			System.out.print("Connecting to SQL Server " + ip + " ... ");
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void update(String queryString) {
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(queryString);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ResultSet getResults() {
		return results;
	}
}