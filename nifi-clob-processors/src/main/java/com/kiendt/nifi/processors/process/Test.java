package com.kiendt.nifi.processors.process;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.sql.Clob;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.StringWriter;

public class Test {
    public static void main(String[] args) {
        String url = "jdbc:oracle:thin:@127.0.0.1:9501:xe";
        String user = "System";
        String password = "snu@123";
        System.setProperty("user.timezone", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));


        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {

//            statement.execute("CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))");
//            statement.execute("INSERT INTO test VALUES (1, 'John Doe')");
//            statement.execute("INSERT INTO test VALUES (2, 'Jane Doe')");

            String selectQuery = "SELECT * FROM clob_test";
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
                    System.out.println(columnName);
                    Object columnValue = resultSet.getObject(i);
                    if (columnValue instanceof Clob){
                        columnValue = convertClobToString((Clob) columnValue);
                    }
                    System.out.println(columnValue.getClass());
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

    // Phương thức chuyển đổi Clob sang String
    public static String convertClobToString(Clob clob) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            int length = (int) clob.length();
            String clobString = clob.getSubString(1, length);
            stringBuilder.append(clobString);
        } catch (SQLException e) {
            throw new SQLException("Lỗi khi chuyển đổi CLOB sang String", e);
        }
        return stringBuilder.toString();
    }

}
