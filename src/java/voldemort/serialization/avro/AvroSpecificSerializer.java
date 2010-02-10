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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import voldemort.serialization.SerializationException;
import voldemort.serialization.Serializer;
import voldemort.serialization.avro.AvroGenericSerializer.SeekableByteArrayInput;

/**
 * Avro serializer uses the avro protocol to serialize objects of a particular
 * class type.
 */
public class AvroSpecificSerializer implements Serializer<Object> {

    private final Class clazz;

    /**
     * Constructor accepting the schema definition as a JSON string.
     * 
     * @param className a class name.
     */
    public AvroSpecificSerializer(String className) {
        try {
            clazz = Class.forName(className);
        } catch(ClassNotFoundException e) {
            throw new SerializationException(e);
        }
    }

    public byte[] toBytes(Object object) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataFileWriter<Object> writer = null;
        try {
            DatumWriter<Object> datumWriter = new SpecificDatumWriter(clazz);

            writer = new DataFileWriter<Object>(SpecificData.get().getSchema(clazz),
                                                output,
                                                datumWriter);
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
            DatumReader<Object> datumReader = null;
            datumReader = new SpecificDatumReader(clazz);
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
}
