package com.github.henryhuang.avrojson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * 
 * 
 * @author <a href="mailto:h1886@outlook.com">Henry Huang</a>
 * @version 9:28:20 PM Nov 20, 2016
 * @since 0.0.1
 */
public class AvroJson {

	/**
	 * convert an avro java object to JSON format string, not pretty.
	 * 
	 * @param avroObject
	 *            avro java Object
	 * @param clazz
	 * @param schema
	 *            avro {@link Schema}
	 * @return avro object's JSON format string, if throw {@link IOException},
	 *         return null
	 */
	public static <T> String avroObjectToJson(T avroObject, Class<T> clazz, Schema schema) throws IOException {
		return avroObjectToJson(avroObject, clazz, schema, false);
	}

	/**
	 * convert an avro java object to JSON format string
	 * 
	 * @param avroObject
	 *            avro java Object
	 * @param clazz
	 * @param schema
	 *            avro {@link Schema}
	 * @param pretty
	 *            pretty if true, else not
	 * @return avro object's JSON format string, if throw {@link IOException},
	 *         return null
	 */
	public static <T> String avroObjectToJson(T avroObject, Class<T> clazz, Schema schema, boolean pretty)
			throws IOException {

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		SpecificDatumWriter<T> specificDatumWriter = new SpecificDatumWriter<T>(clazz);
		specificDatumWriter.write(avroObject, binaryEncoder);
		binaryEncoder.flush();

		byte[] bytesValue = outputStream.toByteArray();
		GenericDatumReader<Object> reader = new GenericDatumReader<Object>(schema);
		DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, output, pretty);
		Decoder decoder = DecoderFactory.get().binaryDecoder(bytesValue, null);
		Object datum = reader.read(null, decoder);
		writer.write(datum, encoder);
		encoder.flush();
		output.flush();
		return new String(output.toByteArray(), "UTF-8");

	}

}