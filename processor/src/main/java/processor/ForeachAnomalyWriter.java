package processor;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.lang.RuntimeException;

public class ForeachAnomalyWriter extends ForeachWriter<Row> {
    private String databaseHost = null;
    private String databaseName = "postgres";
    private String databasePass = null;
    private String databaseUser = null;
    private String databasePort = null;
    private String databaseTable = "anomalies";
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
        // "date", "district", "TOTAL", "FBI", "Percent"

        // Write string to connection
        String start = record.getString(0);
        String end = record.getString(1);
        String district = record.getString(2);
        int total = record.getInt(3);
        int fbi = record.getInt(4);
        float percent = record.getFloat(5);

        String SQL = "INSERT INTO " + databaseTable
                + " as t (day_start, day_end, district, total, fbi, percent) VALUES('" + start + "', '" + end + "', '"
                + district + "', " + Integer.toString(total) + ", " + Integer.toString(fbi) + ", "
                + Float.toString(percent)
                + ") ON conflict on constraint anomalies_pkey DO UPDATE SET total=t.total RETURNING 1 as id;";

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