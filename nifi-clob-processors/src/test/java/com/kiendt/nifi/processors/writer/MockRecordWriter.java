package com.kiendt.nifi.processors.writer;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
    private final String header;
    private final int failAfterN;
    private final boolean quoteValues;
    private final boolean bufferOutput;

    private final RecordSchema writeSchema;

    public MockRecordWriter() {
        this(null);
    }

    public MockRecordWriter(final String header) {
        this(header, true, -1, false, null);
    }

    public MockRecordWriter(final String header, final boolean quoteValues) {
        this(header, quoteValues, false);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final int failAfterN) {
        this(header, quoteValues, failAfterN, false, null);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final boolean bufferOutput) {
        this(header, quoteValues, -1, bufferOutput, null);
    }

    public MockRecordWriter(final String header, final boolean quoteValues, final int failAfterN, final boolean bufferOutput, final RecordSchema writeSchema) {
        this.header = header;
        this.quoteValues = quoteValues;
        this.failAfterN = failAfterN;
        this.bufferOutput = bufferOutput;
        this.writeSchema = writeSchema;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return (writeSchema != null) ? writeSchema : new SimpleRecordSchema(Collections.emptyList());
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream rawOut, Map<String, String> variables) {
        final OutputStream out = bufferOutput ? new BufferedOutputStream(rawOut) : rawOut;

        return new RecordSetWriter() {
            private int recordCount = 0;
            private boolean headerWritten = false;

            private RecordSchema writerSchema = schema;

            @Override
            public void flush() throws IOException {
                out.flush();
            }

            @Override
            public WriteResult write(Record record) throws IOException {
                return null;
            }

            @Override
            public String getMimeType() {
                return "text/plain";
            }

            @Override
            public void close() throws IOException {
                out.close();
            }

            @Override
            public WriteResult write(RecordSet recordSet) throws IOException {
                return null;
            }

            @Override
            public void beginRecordSet() throws IOException {
            }

            @Override
            public WriteResult finishRecordSet() throws IOException {
                return WriteResult.of(recordCount, Collections.emptyMap());
            }
        };
    }
}
