package com.kiendt.nifi.processors.process;

import com.kiendt.nifi.processors.clob.FlowfileProperties;
import com.kiendt.nifi.processors.clob.FlowfileRelationships;
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
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.util.StopWatch;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
            final Integer autoCommit = context.getProperty(FlowfileProperties.AUTO_COMMIT).evaluateAttributeExpressions().asInteger();
            final StopWatch stopWatch = new StopWatch(true);
            final String selectQuery;
        }

    }
}
