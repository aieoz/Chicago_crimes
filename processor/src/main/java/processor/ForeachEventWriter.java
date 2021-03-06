package processor;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.lang.RuntimeException;

public class ForeachEventWriter extends ForeachWriter<Row> {
    private String databaseHost = null;
    private String databaseName = "postgres";
    private String databasePass = null;
    private String databaseUser = null;
    private String databasePort = null;
    private String databaseTable = "crimes";
    Connection conn = null;
    Statement statement = null;

    public boolean open(long partitionId, long version) {
        if (databaseHost == null | databaseName == null | databasePass == null | databaseUser == null
                | databasePort == null) {

            System.out.println(
                    "databaseHost, databaseName, databasePass, databaseUser and databasePort must be specified");
        }

        String url = "jdbc:postgresql://" + databaseHost + ":" + databasePort + "/" + databaseName;

        try {
            conn = DriverManager.getConnection(url, databaseUser, databasePass);
            statement = conn.createStatement();
        } catch (SQLException e) {
            throw new RuntimeException("Cant connect to database");
        }

        return true;
    }

    public void process(Row record) {
        // Open connection

        // Write string to connection
        int year = record.getInt(0);
        int month = record.getInt(1);
        String district = record.getString(2);
        String category = record.getString(3);
        int total = record.getInt(4);
        int domestic = record.getInt(5);
        int arrested = record.getInt(6);
        int fbi = record.getInt(7);

        String SQL = "INSERT INTO " + databaseTable
                + " as t (year, month, district, category, total, domestic, arrested, fbi) VALUES("
                + Integer.toString(year) + ", " + Integer.toString(month) + ", '" + district + "', '" + category + "', "
                + Integer.toString(total) + ", " + Integer.toString(domestic) + ", " + Integer.toString(arrested) + ", "
                + Integer.toString(fbi) + ") ON conflict on constraint " + databaseTable
                + "_pkey do update SET total=t.total+" + Integer.toString(total) + " RETURNING 1 as id;";

        try {
            statement.executeQuery(SQL);
        } catch (SQLException e) {
            throw new RuntimeException("Cant execute query: " + SQL);
        }
    }

    public void close(Throwable errorOrNull) {
        // Close the connection
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        conn = null;
        statement = null;
    }

    public void option(String name, String value) {
        if (name == "dbhost") {
            this.databaseHost = value;
        } else if (name == "dbname") {
            this.databaseName = value;
        } else if (name == "dbpassword") {
            this.databasePass = value;
        } else if (name == "dbuser") {
            this.databaseUser = value;
        } else if (name == "dbport") {
            this.databasePort = value;
        } else if (name == "dbtable") {
            this.databaseTable = value;
        }
    }
}