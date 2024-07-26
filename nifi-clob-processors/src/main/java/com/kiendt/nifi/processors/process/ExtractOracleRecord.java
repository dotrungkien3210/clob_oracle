package com.kiendt.nifi.processors.process;

import com.kiendt.nifi.processors.clob.FlowfileProperties;
import com.kiendt.nifi.processors.clob.FlowfileRelationships;
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
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ExtractOracleRecord extends AbstractProcessor {


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(FlowfileProperties.DBCP_SERVICE);
        descriptors.add(FlowfileProperties.SQL_SELECT_QUERY);
        descriptors.add(FlowfileProperties.QUERY_TIMEOUT);
        descriptors.add(FlowfileProperties.MAX_ROWS_PER_FLOW_FILE);
        descriptors.add(FlowfileProperties.OUTPUT_BATCH_SIZE);
        descriptors.add(FlowfileProperties.FETCH_SIZE);
        descriptors.add(FlowfileProperties.AUTO_COMMIT);
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
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }

            final ComponentLog logger = getLogger();
            final DBCPService dbcpService = context.getProperty(FlowfileProperties.DBCP_SERVICE).asControllerService(DBCPService.class);
            final Integer queryTimeout = context.getProperty(FlowfileProperties.QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
            final boolean maxRowPerFlowfile = context.getProperty(FlowfileProperties.MAX_ROWS_PER_FLOW_FILE).asBoolean();
            final Boolean outputBatchSize = context.getProperty(FlowfileProperties.OUTPUT_BATCH_SIZE).asBoolean();
            final Integer fetchSize = context.getProperty(FlowfileProperties.FETCH_SIZE).evaluateAttributeExpressions().asInteger();
            final StopWatch stopWatch = new StopWatch(true);
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
            SQLProcessing sqlProcessing = new SQLProcessing();

            try (final Connection con = dbcpService.getConnection();
                 final Statement st = con.createStatement()) {
                st.setQueryTimeout(queryTimeout); // timeout in seconds
                final AtomicLong nrOfRows = new AtomicLong(0L);
                if (fileToProcess == null) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        try {
                            logger.debug("Executing query {}", new Object[]{selectQuery});
                            final ResultSet resultSet = st.executeQuery(selectQuery);
                            final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                                    .convertNames(convertNamesForAvro)
                                    .useLogicalTypes(useAvroLogicalTypes)
                                    .defaultPrecision(defaultPrecision)
                                    .defaultScale(defaultScale)
                                    .build();
                            nrOfRows.set(JdbcCommon.convertToAvroStream(resultSet, out, options, null));
                        } catch (final SQLException e) {
                            throw new ProcessException(e);
                        }
                    }
                });

                // set attribute how many rows were selected
                fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);

                logger.info("{} contains {} Avro records; transferring to 'success'",
                        new Object[]{fileToProcess, nrOfRows.get()});
                session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved " + nrOfRows.get() + " rows",
                        stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(fileToProcess, REL_SUCCESS);
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
                    session.transfer(fileToProcess, REL_FAILURE);
                }
            }
        }
    }
}
