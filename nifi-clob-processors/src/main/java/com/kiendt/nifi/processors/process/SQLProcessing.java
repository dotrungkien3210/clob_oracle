package com.kiendt.nifi.processors.process;

import com.kiendt.nifi.processors.clob.FlowfileProperties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;
import org.apache.nifi.util.db.SensitiveValueWrapper;

import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.db.JdbcCommon;
import java.util.concurrent.atomic.AtomicLong;
import java.sql.Connection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.nifi.processor.io.OutputStreamCallback;
public class SQLProcessing {


    public Connection checkConnection(Connection con, ProcessContext context, ComponentLog logger) throws SQLException {
        final boolean isAutoCommit = con.getAutoCommit();
        final boolean setAutoCommitValue = context.getProperty(FlowfileProperties.AUTO_COMMIT).asBoolean();
        // Only set auto-commit if necessary, log any "feature not supported" exceptions
        if (isAutoCommit != setAutoCommitValue) {
            try {
                con.setAutoCommit(setAutoCommitValue);
            } catch (SQLFeatureNotSupportedException sfnse) {
                logger.debug("setAutoCommit({}) not supported by this driver", setAutoCommitValue);
            }
        }
        return con;
    }

    public void executeSQL(){
        while (hasResults || hasUpdateCount) {
            //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
            if (hasResults) {
                final AtomicLong nrOfRows = new AtomicLong(0L);

                try {
                    final ResultSet resultSet = st.getResultSet();
                    do {
                        final StopWatch fetchTime = new StopWatch(true);

                        FlowFile resultSetFF;
                        if (fileToProcess == null) {
                            resultSetFF = session.create();
                        } else {
                            resultSetFF = session.create(fileToProcess);
                        }

                        try {
                            resultSetFF = session.write(resultSetFF, out -> {
                                try {
                                    nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), null));
                                } catch (Exception e) {
                                    throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
                                }
                            });

                            // if fragmented ResultSet, determine if we should keep this fragment
                            if (maxRowsPerFlowFile > 0 && nrOfRows.get() == 0 && fragmentIndex > 0) {
                                // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                session.remove(resultSetFF);
                                break;
                            }

                            long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                            // set attributes
                            final Map<String, String> attributesToAdd = new HashMap<>();
                            if (inputFileAttrMap != null) {
                                attributesToAdd.putAll(inputFileAttrMap);
                            }
                            attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                            attributesToAdd.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                            attributesToAdd.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                            attributesToAdd.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
                            attributesToAdd.put(RESULTSET_INDEX, String.valueOf(resultCount));
                            if (inputFileUUID != null) {
                                attributesToAdd.put(INPUT_FLOWFILE_UUID, inputFileUUID);
                            }
                            if (maxRowsPerFlowFile > 0) {
                                // if fragmented ResultSet, set fragment attributes
                                attributesToAdd.put(FRAGMENT_ID, fragmentId);
                                attributesToAdd.put(FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                            }
                            attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                            resultSetFF = session.putAllAttributes(resultSetFF, attributesToAdd);
                            sqlWriter.updateCounters(session);

                            logger.info("{} contains {} records; transferring to 'success'", resultSetFF, nrOfRows.get());

                            // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
                            if (context.hasIncomingConnection()) {
                                session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                            } else {
                                session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                            }
                            resultSetFlowFiles.add(resultSetFF);

                            // If we've reached the batch size, send out the flow files
                            if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                                session.transfer(resultSetFlowFiles, REL_SUCCESS);
                                // Need to remove the original input file if it exists
                                if (fileToProcess != null) {
                                    session.remove(fileToProcess);
                                    fileToProcess = null;
                                }

                                session.commitAsync();
                                resultSetFlowFiles.clear();
                            }

                            fragmentIndex++;
                        } catch (Exception e) {
                            // Remove any result set flow file(s) and propagate the exception
                            session.remove(resultSetFF);
                            session.remove(resultSetFlowFiles);
                            if (e instanceof ProcessException) {
                                throw (ProcessException) e;
                            } else {
                                throw new ProcessException(e);
                            }
                        }
                    } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);

                    // If we are splitting results but not outputting batches, set count on all FlowFiles
                    if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
                        for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                            resultSetFlowFiles.set(i,
                                    session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                        }
                    }
                } catch (final SQLException e) {
                    throw new ProcessException(e);
                }

                resultCount++;
            }

            // are there anymore result sets?
            try {
                hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                hasUpdateCount = st.getUpdateCount() != -1;
            } catch (SQLException ex) {
                hasResults = false;
                hasUpdateCount = false;
            }
        }
    }


    public void executeSqlQuery(Connection con,Integer fetchSize,String selectQuery,ComponentLog logger) throws SQLException {
            try (final PreparedStatement st = con.prepareStatement(selectQuery)) {
                if (fetchSize != null && fetchSize > 0) {
                    try {
                        st.setFetchSize(fetchSize);
                    } catch (SQLException se) {
                        // Not all drivers support this, just log the error (at debug level) and move on
                        logger.info("Cannot set fetch size to {} due to {}", fetchSize, se.getLocalizedMessage(), se);
                    }
                }
                st.setQueryTimeout(queryTimeout); // timeout in seconds

                // Execute pre-query, throw exception and cleanup Flow Files if fail
                Pair<String, SQLException> failure = executeConfigStatements(con, preQueries);
                if (failure != null) {
                    // In case of failure, assigning config query to "selectQuery" to follow current error handling
                    selectQuery = failure.getLeft();
                    throw failure.getRight();
                }

                final Map<String, SensitiveValueWrapper> sqlParameters = context.getProperties()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getKey().isDynamic())
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> new SensitiveValueWrapper(e.getValue(), e.getKey().isSensitive())));

                if (fileToProcess != null) {
                    for (Map.Entry<String, String> entry : fileToProcess.getAttributes().entrySet()) {
                        sqlParameters.put(entry.getKey(), new SensitiveValueWrapper(entry.getValue(), false));
                    }
                }

                if (!sqlParameters.isEmpty()) {
                    JdbcCommon.setSensitiveParameters(st, sqlParameters);
                }

                logger.debug("Executing query {}", selectQuery);

                int fragmentIndex = 0;
                final String fragmentId = UUID.randomUUID().toString();

                final StopWatch executionTime = new StopWatch(true);

                boolean hasResults = st.execute();

                long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

                boolean hasUpdateCount = st.getUpdateCount() != -1;

                Map<String, String> inputFileAttrMap = fileToProcess == null ? null : fileToProcess.getAttributes();
                String inputFileUUID = fileToProcess == null ? null : fileToProcess.getAttribute(CoreAttributes.UUID.key());


                // Execute post-query, throw exception and cleanup Flow Files if fail
                failure = executeConfigStatements(con, postQueries);
                if (failure != null) {
                    selectQuery = failure.getLeft();
                    resultSetFlowFiles.forEach(session::remove);
                    throw failure.getRight();
                }

                // If the auto commit is set to false, commit() is called for consistency
                if (!con.getAutoCommit()) {
                    con.commit();
                }

                // Transfer any remaining files to SUCCESS
                session.transfer(resultSetFlowFiles, REL_SUCCESS);
                resultSetFlowFiles.clear();

                if (fileToProcess != null) {
                    if (resultCount > 0) {
                        // If we had at least one result then it's OK to drop the original file
                        session.remove(fileToProcess);
                    } else {
                        // If we had no results then transfer the original flow file downstream to trigger processors
                        session.transfer(setFlowFileEmptyResults(session, fileToProcess, sqlWriter), REL_SUCCESS);
                    }
                } else if (resultCount == 0) {
                    // If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
                    // Then generate an empty Output FlowFile
                    FlowFile resultSetFF = session.create();
                    session.transfer(setFlowFileEmptyResults(session, resultSetFF, sqlWriter), REL_SUCCESS);
                }
            }

    }
}
