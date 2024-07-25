package com.kiendt.nifi.processors.clob;

import org.apache.nifi.processor.Relationship;

import java.util.Set;

public class FlowfileRelationships {
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    protected Set<Relationship> relationships;
}
