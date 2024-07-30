package com.kiendt.nifi.processors.process;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.kiendt.nifi.processors.clob.FlowfileProperties;
import com.kiendt.nifi.processors.clob.FlowfileRelationships;
import com.kiendt.nifi.processors.util.FormatStream;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class ExtractOracleRecord extends AbstractProcessor {
    public static final String RESULT_ROW_COUNT = "executesql.row.count";

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        System.setProperty("user.timezone", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        descriptors = new ArrayList<>();
        descriptors.add(FlowfileProperties.DBCP_SERVICE);
        descriptors.add(FlowfileProperties.SQL_SELECT_QUERY);
        descriptors.add(FlowfileProperties.QUERY_TIMEOUT);
        descriptors.add(FlowfileProperties.AUTO_COMMIT);
        descriptors.add(FlowfileProperties.AVRO_OPTION);
        descriptors.add(FlowfileProperties.AVRO_SCHEMA);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(FlowfileRelationships.REL_SUCCESS);
        relationships.add(FlowfileRelationships.REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {


        final ComponentLog logger = getLogger();
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        // lấy các properties tại đây
        final DBCPService dbcpService = context.getProperty(FlowfileProperties.DBCP_SERVICE).asControllerService(DBCPService.class);
        final int queryTimeout = context.getProperty(FlowfileProperties.QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final StopWatch stopWatch = new StopWatch(true);
        final String schemaString = context.getProperty(FlowfileProperties.AVRO_SCHEMA).evaluateAttributeExpressions(fileToProcess).getValue();
        final String schemaOption = context.getProperty(FlowfileProperties.AVRO_OPTION).evaluateAttributeExpressions(fileToProcess).getValue();

        final String selectQuery;

        // get select query

        if (context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in)));
            selectQuery = queryContents.toString();
        }


        try (final Connection con = dbcpService.getConnection(); final Statement st = con.createStatement()) {

            st.setQueryTimeout(queryTimeout); // timeout in seconds
            final AtomicLong nrOfRows = new AtomicLong(0L);
            if (fileToProcess == null) {
                fileToProcess = session.create();
            }
            fileToProcess = session.write(fileToProcess, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    JsonFactory jsonFactory = new JsonFactory().setRootValueSeparator(null);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    JsonParser jsonParser;
                    JsonGenerator jsonGen = jsonFactory.createGenerator(baos);
                    Schema schema = null;

                    try {
                        logger.debug("Executing query {}", new Object[]{selectQuery});
                        final ResultSet resultSet = st.executeQuery(selectQuery);
                        String jsonString = resultSetToJson(resultSet);
                        logger.info("sau khi fetch: " + jsonString);


                        if (schemaOption.equals("true")) {
                            try {
                                schema = new Schema.Parser().parse(schemaString);
                            } catch (NullPointerException e) {
                                schema = FormatStream.getEmbeddedSchema(in);
                                in.reset();
                            }
                            jsonGen.flush();
                            baos = FormatStream.jsonToAvro(baos, schema);
                            baos.writeTo(out);
                        }

//                        nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, options, null));

//                        out.write(jsonString.getBytes());
//                        IOUtils.write(jsonString, out, "UTF-8");

//                        jsonGen.writeString(jsonString);
//                        jsonGen.flush();
//                        out.write(jsonString.getBytes());
//                        baos.writeTo(out);


                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }
                }
            });

            // set attribute how many rows were selected
            fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
            fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);

            logger.info("{} contains {} records; transferring to 'success'",
                    new Object[]{fileToProcess, nrOfRows.get()});
            session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                    stopWatch.getElapsed(TimeUnit.MILLISECONDS));

            session.transfer(fileToProcess, FlowfileRelationships.REL_SUCCESS);


        } catch (final ProcessException | SQLException e) {
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.transfer(fileToProcess, FlowfileRelationships.REL_FAILURE);
            }
        }
    }

    // Đưa về

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
                    if (columnValue instanceof Clob) {
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
            throw new SQLException("ERROR When CLOB sang String", e);
        }
        return stringBuilder.toString();
    }
}