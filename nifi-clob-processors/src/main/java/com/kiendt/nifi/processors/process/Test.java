package com.kiendt.nifi.processors.process;

public class Test {

    public void process(final OutputStream out) throws IOException {
        try {
            logger.debug("Executing query {}", selectQuery);
            final ResultSet resultSet = st.executeQuery(selectQuery);
            List<Map<String, Object>> records = convertToRecords(resultSet);
            writeRecordsToOutput(records, out);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    private List<Map<String, Object>> convertToRecords(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> records = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> record = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                record.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            records.add(record);
        }

        return records;
    }

    private void writeRecordsToOutput(List<Map<String, Object>> records, OutputStream out) throws IOException {
        try (PrintWriter writer = new PrintWriter(out)) {
            for (Map<String, Object> record : records) {
                writer.println(record);
            }
        }
    }
}
