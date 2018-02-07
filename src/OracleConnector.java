import java.sql.*;
import java.util.Properties;
import oracle.jdbc.driver.*;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleDriver;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.DatabaseChangeRegistration;

public class OracleConnector {
    static final String USERNAME = "test";
    static final String PASSWORD = "test";
    private String connectionUrl[] = {"jdbc:sqlserver://", ":1433;DatabaseName=NBS_ODSE"};
    private ResultSet results;
    private OracleConnection connection;

    public OracleConnector(String ip) throws SQLException {
        // Load SQL Server JDBC driver and establish connection.
        try{
            OracleDriver dr = new OracleDriver();
            Properties prop = new Properties();
            prop.setProperty("user", OracleConnector.USERNAME);
            prop.setProperty("password", OracleConnector.PASSWORD);
            String url = connectionUrl[0] + ip + connectionUrl[1];
            System.out.print("Connecting to SQL Server url " + ip + " ... ");
            connection = (OracleConnection) dr.connect(url, prop);
            System.out.println("Connected.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void query(String queryString) throws Exception {
        Properties prop = new Properties();
        prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
        DatabaseChangeRegistration dcr = connection.registerDatabaseChangeNotification(prop);

        try {
            dcr.addListener(new DatabaseChangeListener() {

                public void onDatabaseChangeNotification(DatabaseChangeEvent dce) {
                    System.out.println("Changed row id : "+dce.getTableChangeDescription()[0].getRowChangeDescription()[0].getRowid().stringValue());
                }
            });

            Statement statement = connection.createStatement();
            ((OracleStatement) statement).setDatabaseChangeRegistration(dcr);
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

    public static void main(String[] args) throws Exception {
        OracleConnector test = new OracleConnector("128.61.22.209");
        try {
            test.query("SELECT person_uid, add_reason_cd FROM NBS_ODSE.dbo.Person");
            System.out.println(test.getResults());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}