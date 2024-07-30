package com.kiendt.nifi.processors.clob;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public class FlowfileProperties {

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connectiecordson Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();


    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .description("The SQL select query to execute. The query can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("esql-auto-commit")
            .displayName("Set Auto Commit")
            .description("Enables or disables the auto commit functionality of the DB connection. Default value is 'true'. " +
                    "The default value can be used with most of the JDBC drivers and this functionality doesn't have any impact in most of the cases " +
                    "since this processor is used to read data. " +
                    "However, for some JDBC drivers such as PostgreSQL driver, it is required to disable the auto committing functionality " +
                    "to limit the number of result rows fetching at a time. " +
                    "When auto commit is enabled, postgreSQL driver loads whole result set to memory at once. " +
                    "This could lead for a large amount of memory usage when executing queries which fetch large data sets. " +
                    "More Details of this behaviour in PostgreSQL driver can be found in https://jdbc.postgresql.org//documentation/head/query.html. ")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor
            .Builder().name("AVRO_SCHEMA")
            .displayName("Avro Schema")
            .description("Specify the schema if the FlowFile format is Avro.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AVRO_OPTION = new PropertyDescriptor
            .Builder().name("AVRO_OPTION")
            .displayName("Avro Option")
            .description("Choose option input.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

}
