/*
 * Copyright 2010 Antoine Toulme
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.serialization.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;

/**
 * AvroSerializer uses the avro protocol to serialize objects
 * 
 */
public class AvroGenericSerializer implements Serializer<Object> {

    private final Schema typeDef;

    /**
     * Constructor accepting the schema definition as a JSON string.
     * 
     * @param schema a serialized JSON object representing a Avro schema.
     */
    public AvroGenericSerializer(String schema) {
        typeDef = Schema.parse(schema);
    }

    public byte[] toBytes(Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataFileWriter<Object> writer = null;
        try {
            DatumWriter<Object> datumWriter = new GenericDatumWriter<Object>(typeDef);

            writer = new DataFileWriter<Object>(datumWriter).create(typeDef, output);
            writer.append(object);
            writer.flush();
            return output.toByteArray();
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch(IOException e) {}
            }
        }
    }

    public Object toObject(byte[] bytes) {
        SeekableByteArrayInput input = new SeekableByteArrayInput(bytes);
        DataFileReader<Object> reader = null;
        try {
            DatumReader<Object> datumReader = new GenericDatumReader<Object>(typeDef);
            reader = new DataFileReader<Object>(input, datumReader);
            return reader.next(null);
        } catch(IOException e) {
            throw new SerializationException(e);
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                } catch(IOException e) {}
            }
        }
    }

    /**
     * A simple implementation of the SeekableInput for a ByteArrayInputStream.
     * 
     * @author antoine
     */
    static class SeekableByteArrayInput extends ByteArrayInputStream implements SeekableInput {

        public SeekableByteArrayInput(byte[] buf) {
            super(buf);
        }

        public long length() throws IOException {
            return buf.length;
        }

        public void seek(long p) throws IOException {
            pos = (int) p;
        }

        public long tell() throws IOException {
            return pos;
        }

    }

}
