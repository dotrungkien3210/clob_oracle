//package com.kiendt.nifi.processors.process;
//
//import java.nio.charset.Charset;
//import java.sql.*;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.stream.Collectors;
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.nifi.annotation.lifecycle.OnScheduled;
//import org.apache.nifi.dbcp.DBCPService;
//import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.flowfile.attributes.CoreAttributes;
//import org.apache.nifi.flowfile.attributes.FragmentAttributes;
//import org.apache.nifi.logging.ComponentLog;
//import org.apache.nifi.processor.AbstractProcessor;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processor.ProcessSession;
//import org.apache.nifi.processor.exception.ProcessException;
//import org.apache.nifi.processors.standard.sql.SqlWriter;
//import org.apache.nifi.util.StopWatch;
//import org.apache.nifi.util.db.JdbcCommon;
//import org.apache.nifi.util.db.SensitiveValueWrapper;
//import com.kiendt.nifi.processors.clob.FlowfileRelationships;
//import com.kiendt.nifi.processors.clob.FlowfileProperties;
//
//public abstract class AbstractExecuteSQL extends AbstractProcessor {
//
//    public static final String RESULT_ROW_COUNT = "executesql.row.count";
//    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";
//    public static final String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
//    public static final String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
//    public static final String RESULTSET_INDEX = "executesql.resultset.index";
//    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";
//    public static final String INPUT_FLOWFILE_UUID = "input.flowfile.uuid";
//
//    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
//    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
//    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
//
//    protected DBCPService dbcpService;
//
//
//
//    @OnScheduled
//    public void setup(ProcessContext context) {
//        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
//        if (!context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
//            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
//                    + "providing flowfile(s) containing a SQL select query";
//            getLogger().error(errorString);
//            throw new ProcessException(errorString);
//        }
//        dbcpService = context.getProperty(FlowfileProperties.DBCP_SERVICE).asControllerService(DBCPService.class);
//
//    }
//
//    @Override
//    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
//        FlowFile fileToProcess = null;
//        if (context.hasIncomingConnection()) {
//            fileToProcess = session.get();
//
//            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
//            // However, if we have no FlowFile and we have connections coming from other Processors, then
//            // we know that we should run only if we have a FlowFile.
//            if (fileToProcess == null && context.hasNonLoopConnection()) {
//                return;
//            }
//        }
//
//        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();
//
//        final ComponentLog logger = getLogger();
//        final int queryTimeout = context.getProperty(FlowfileProperties.QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
//        final Integer maxRowsPerFlowFile = context.getProperty(FlowfileProperties.MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
//        final Integer outputBatchSizeField = context.getProperty(FlowfileProperties.OUTPUT_BATCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
//        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
//        final Integer fetchSize = context.getProperty(FlowfileProperties.FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
//
//        List<String> preQueries = getQueries(context.getProperty(FlowfileProperties.SQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
//        List<String> postQueries = getQueries(context.getProperty(FlowfileProperties.SQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
//
//        SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);
//
//        String selectQuery;
//        if (context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).isSet()) {
//            selectQuery = context.getProperty(FlowfileProperties.SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
//        } else {
//            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
//            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
//            final StringBuilder queryContents = new StringBuilder();
//            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
//            selectQuery = queryContents.toString();
//        }
//
//        int resultCount = 0;
//        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes())) {
//            final boolean isAutoCommit = con.getAutoCommit();
//            final boolean setAutoCommitValue = context.getProperty(FlowfileProperties.AUTO_COMMIT).asBoolean();
//            // Only set auto-commit if necessary, log any "feature not supported" exceptions
//            if (isAutoCommit != setAutoCommitValue) {
//                try {
//                    con.setAutoCommit(setAutoCommitValue);
//                } catch (SQLFeatureNotSupportedException sfnse) {
//                    logger.debug("setAutoCommit({}) not supported by this driver", setAutoCommitValue);
//                }
//            }
//            try (final PreparedStatement st = con.prepareStatement(selectQuery)) {
//                if (fetchSize != null && fetchSize > 0) {
//                    try {
//                        st.setFetchSize(fetchSize);
//                    } catch (SQLException se) {
//                        // Not all drivers support this, just log the error (at debug level) and move on
//                        logger.debug("Cannot set fetch size to {} due to {}", fetchSize, se.getLocalizedMessage(), se);
//                    }
//                }
//                st.setQueryTimeout(queryTimeout); // timeout in seconds
//
//                // Execute pre-query, throw exception and cleanup Flow Files if fail
//                Pair<String, SQLException> failure = executeConfigStatements(con, preQueries);
//                if (failure != null) {
//                    // In case of failure, assigning config query to "selectQuery" to follow current error handling
//                    selectQuery = failure.getLeft();
//                    throw failure.getRight();
//                }
//
//                final Map<String, SensitiveValueWrapper> sqlParameters = context.getProperties()
//                        .entrySet()
//                        .stream()
//                        .filter(e -> e.getKey().isDynamic())
//                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> new SensitiveValueWrapper(e.getValue(), e.getKey().isSensitive())));
//
//                if (fileToProcess != null) {
//                    for (Map.Entry<String, String> entry : fileToProcess.getAttributes().entrySet()) {
//                        sqlParameters.put(entry.getKey(), new SensitiveValueWrapper(entry.getValue(), false));
//                    }
//                }
//
//                if (!sqlParameters.isEmpty()) {
//                    JdbcCommon.setSensitiveParameters(st, sqlParameters);
//                }
//
//                logger.debug("Executing query {}", selectQuery);
//
//                int fragmentIndex = 0;
//                final String fragmentId = UUID.randomUUID().toString();
//
//                final StopWatch executionTime = new StopWatch(true);
//
//                boolean hasResults = st.execute();
//
//                long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);
//
//                boolean hasUpdateCount = st.getUpdateCount() != -1;
//
//                Map<String, String> inputFileAttrMap = fileToProcess == null ? null : fileToProcess.getAttributes();
//                String inputFileUUID = fileToProcess == null ? null : fileToProcess.getAttribute(CoreAttributes.UUID.key());
//                while (hasResults || hasUpdateCount) {
//                    //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
//                    if (hasResults) {
//                        final AtomicLong nrOfRows = new AtomicLong(0L);
//
//                        try {
//                            final ResultSet resultSet = st.getResultSet();
//                            do {
//                                final StopWatch fetchTime = new StopWatch(true);
//
//                                FlowFile resultSetFF;
//                                if (fileToProcess == null) {
//                                    resultSetFF = session.create();
//                                } else {
//                                    resultSetFF = session.create(fileToProcess);
//                                }
//
//                                try {
//                                    resultSetFF = session.write(resultSetFF, out -> {
//                                        try {
//                                            nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), null));
//                                        } catch (Exception e) {
//                                            throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
//                                        }
//                                    });
//
//                                    // if fragmented ResultSet, determine if we should keep this fragment
//                                    if (maxRowsPerFlowFile > 0 && nrOfRows.get() == 0 && fragmentIndex > 0) {
//                                        // if row count is zero and this is not the first fragment, drop it instead of committing it.
//                                        session.remove(resultSetFF);
//                                        break;
//                                    }
//
//                                    long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);
//
//                                    // set attributes
//                                    final Map<String, String> attributesToAdd = new HashMap<>();
//                                    if (inputFileAttrMap != null) {
//                                        attributesToAdd.putAll(inputFileAttrMap);
//                                    }
//                                    attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
//                                    attributesToAdd.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
//                                    attributesToAdd.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
//                                    attributesToAdd.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
//                                    attributesToAdd.put(RESULTSET_INDEX, String.valueOf(resultCount));
//                                    if (inputFileUUID != null) {
//                                        attributesToAdd.put(INPUT_FLOWFILE_UUID, inputFileUUID);
//                                    }
//                                    if (maxRowsPerFlowFile > 0) {
//                                        // if fragmented ResultSet, set fragment attributes
//                                        attributesToAdd.put(FRAGMENT_ID, fragmentId);
//                                        attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
//                                    }
//                                    attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
//                                    resultSetFF = session.putAllAttributes(resultSetFF, attributesToAdd);
//                                    sqlWriter.updateCounters(session);
//
//                                    logger.info("{} contains {} records; transferring to 'success'", resultSetFF, nrOfRows.get());
//
//                                    // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
//                                    if (context.hasIncomingConnection()) {
//                                        session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
//                                    } else {
//                                        session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
//                                    }
//                                    resultSetFlowFiles.add(resultSetFF);
//
//                                    // If we've reached the batch size, send out the flow files
//                                    if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
//                                        session.transfer(resultSetFlowFiles, FlowfileRelationships.REL_SUCCESS);
//                                        // Need to remove the original input file if it exists
//                                        if (fileToProcess != null) {
//                                            session.remove(fileToProcess);
//                                            fileToProcess = null;
//                                        }
//
//                                        session.commitAsync();
//                                        resultSetFlowFiles.clear();
//                                    }
//
//                                    fragmentIndex++;
//                                } catch (Exception e) {
//                                    // Remove any result set flow file(s) and propagate the exception
//                                    session.remove(resultSetFF);
//                                    session.remove(resultSetFlowFiles);
//                                    if (e instanceof ProcessException) {
//                                        throw (ProcessException) e;
//                                    } else {
//                                        throw new ProcessException(e);
//                                    }
//                                }
//                            } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);
//
//                            // If we are splitting results but not outputting batches, set count on all FlowFiles
//                            if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
//                                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
//                                    resultSetFlowFiles.set(i,
//                                            session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
//                                }
//                            }
//                        } catch (final SQLException e) {
//                            throw new ProcessException(e);
//                        }
//
//                        resultCount++;
//                    }
//
//                    // are there anymore result sets?
//                    try {
//                        hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
//                        hasUpdateCount = st.getUpdateCount() != -1;
//                    } catch (SQLException ex) {
//                        hasResults = false;
//                        hasUpdateCount = false;
//                    }
//                }
//
//                // Execute post-query, throw exception and cleanup Flow Files if fail
//                failure = executeConfigStatements(con, postQueries);
//                if (failure != null) {
//                    selectQuery = failure.getLeft();
//                    resultSetFlowFiles.forEach(session::remove);
//                    throw failure.getRight();
//                }
//
//                // If the auto commit is set to false, commit() is called for consistency
//                if (!con.getAutoCommit()) {
//                    con.commit();
//                }
//
//                // Transfer any remaining files to SUCCESS
//                session.transfer(resultSetFlowFiles, FlowfileRelationships.REL_SUCCESS);
//                resultSetFlowFiles.clear();
//
//                if (fileToProcess != null) {
//                    if (resultCount > 0) {
//                        // If we had at least one result then it's OK to drop the original file
//                        session.remove(fileToProcess);
//                    } else {
//                        // If we had no results then transfer the original flow file downstream to trigger processors
//                        session.transfer(setFlowFileEmptyResults(session, fileToProcess, sqlWriter), FlowfileRelationships.REL_SUCCESS);
//                    }
//                } else if (resultCount == 0) {
//                    // If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
//                    // Then generate an empty Output FlowFile
//                    FlowFile resultSetFF = session.create();
//                    session.transfer(setFlowFileEmptyResults(session, resultSetFF, sqlWriter), FlowfileRelationships.REL_SUCCESS);
//                }
//            }
//        } catch (final ProcessException | SQLException e) {
//            //If we had at least one result then it's OK to drop the original file, but if we had no results then
//            //  pass the original flow file down the line to trigger downstream processors
//            if (fileToProcess == null) {
//                // This can happen if any exceptions occur while setting up the connection, statement, etc.
//                logger.error("Unable to execute SQL select query [{}]. No FlowFile to route to failure", selectQuery, e);
//                context.yield();
//            } else {
//                if (context.hasIncomingConnection()) {
//                    logger.error("Unable to execute SQL select query [{}] for {} routing to failure", selectQuery, fileToProcess, e);
//                    fileToProcess = session.penalize(fileToProcess);
//                } else {
//                    logger.error("Unable to execute SQL select query [{}] routing to failure", selectQuery, e);
//                    context.yield();
//                }
//                session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, e.getMessage());
//                session.transfer(fileToProcess, FlowfileRelationships.REL_FAILURE);
//            }
//        }
//    }
//
//    protected FlowFile setFlowFileEmptyResults(final ProcessSession session, FlowFile flowFile, SqlWriter sqlWriter) {
//        flowFile = session.write(flowFile, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
//        final Map<String, String> attributesToAdd = new HashMap<>();
//        attributesToAdd.put(RESULT_ROW_COUNT, "0");
//        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
//        return session.putAllAttributes(flowFile, attributesToAdd);
//    }
//
//    /*
//     * Executes given queries using pre-defined connection.
//     * Returns null on success, or a query string if failed.
//     */
//    protected Pair<String, SQLException> executeConfigStatements(final Connection con, final List<String> configQueries) {
//        if (configQueries == null || configQueries.isEmpty()) {
//            return null;
//        }
//
//        for (String confSQL : configQueries) {
//            try (final Statement st = con.createStatement()) {
//                st.execute(confSQL);
//            } catch (SQLException e) {
//                return Pair.of(confSQL, e);
//            }
//        }
//        return null;
//    }
//
//    /*
//     * Extract list of queries from config property
//     */
//    protected List<String> getQueries(final String value) {
//        if (value == null || value.isEmpty() || value.trim().isEmpty()) {
//            return null;
//        }
//        final List<String> queries = new LinkedList<>();
//        for (String query : value.split("(?<!\\\\);")) {
//            query = query.replaceAll("\\\\;", ";");
//            if (!query.trim().isEmpty()) {
//                queries.add(query.trim());
//            }
//        }
//        return queries;
//    }
//
//    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess);
//}