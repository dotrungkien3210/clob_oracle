package com.kiendt.nifi.processors.util;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.ExtendedGenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormatStream {

    private static final Logger logger = LoggerFactory.getLogger(FormatStream.class);

    public static InputStream avroToJson(InputStream in, Schema schema) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileStream<Object> streamReader = new DataFileStream<Object>(in, reader);
        DatumWriter<Object> writer = new ExtendedGenericDatumWriter<>(schema);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedJsonEncoder encoder = new ExtendedJsonEncoder(schema, baos);

        for (Object datum : streamReader)
            writer.write(datum, encoder);

        encoder.flush();
        baos.flush();
        streamReader.close();

        return convertStream(baos);
    }

    public static ByteArrayOutputStream jsonToAvro(ByteArrayOutputStream jsonStream, Schema schema) throws IOException {
        InputStream input = convertStream(jsonStream);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        DataFileWriter<Object> writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
        writer.setCodec(CodecFactory.snappyCodec());
        writer.create(schema, baos);

        Decoder decoder = new ExtendedJsonDecoder(schema, input);
        Object datum;

        while (true) {
            try {
                datum = reader.read(null, decoder);
            } catch (EOFException eofe) {
                break;
            }
            writer.append(datum);
        }

        writer.close();
        input.close();

        return baos;
    }

    public static Schema getEmbeddedSchema(InputStream in) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<Object>();
        DataFileStream<Object> streamReader = new DataFileStream<Object>(in, reader);
        streamReader.close();

        return streamReader.getSchema();
    }

    private static InputStream convertStream(ByteArrayOutputStream baos) throws IOException {
        PipedInputStream pin = new PipedInputStream();
        PipedOutputStream pout = new PipedOutputStream(pin);

        new Thread(
                new Runnable() {
                    public void run() {
                        try {
                            baos.writeTo(pout);
                            pout.close();
                        } catch (IOException e) {
                            logger.error(e.getMessage());
                        }
                    }
                }
        ).start();

        return pin;
    }
}