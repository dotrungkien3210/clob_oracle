package com.kiendt.nifi.processors.process;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@localhost:1521:xe";
        String user = "sys as sysdba";
        String password = "trungkien123";

        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {

//            statement.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))");
//            statement.execute("INSERT INTO test VALUES (1, 'John Doe')");
//            statement.execute("INSERT INTO test VALUES (2, 'Jane Doe')");

            String selectQuery = "SELECT * FROM test";
            AtomicInteger nrOfRows = new AtomicInteger(0);

            FileWriter out = new FileWriter("output.json");
            try {

                ResultSet resultSet = statement.executeQuery(selectQuery);
                String result = resultSetToJson(resultSet);
                System.out.println(result);
//                nrOfRows.set(2); // Simulate number of rows processed
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            out.close();
            System.out.println("Number of rows processed: " + nrOfRows.get());

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }



    public static String resultSetToJson(ResultSet resultSet) throws SQLException {
        StringWriter writer = new StringWriter();
        JsonFactory jsonFactory = new JsonFactory();

        try (JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer)) {
            jsonGenerator.writeStartArray();
            int columnCount = resultSet.getMetaData().getColumnCount();

            while (resultSet.next()) {
                jsonGenerator.writeStartObject();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSet.getMetaData().getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    jsonGenerator.writeObjectField(columnName, columnValue);
                }
                jsonGenerator.writeEndObject();
            }

            jsonGenerator.writeEndArray();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writer.toString();
    }
}
