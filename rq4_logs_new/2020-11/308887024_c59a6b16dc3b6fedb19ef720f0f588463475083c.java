package com.marketplace.context.mongo.codec;

import com.marketplace.domain.classifiedad.ClassifiedAdText;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

public class ClassifiedAdTextCodec implements Codec<ClassifiedAdText> {
    @Override
    public ClassifiedAdText decode(BsonReader reader, DecoderContext decoderContext) {
        String s = reader.readString();
        return ClassifiedAdText.fromString(s);
    }

    @Override
    public void encode(BsonWriter writer, ClassifiedAdText value, EncoderContext encoderContext) {
        writer.writeString(value.value());
    }

    @Override
    public Class<ClassifiedAdText> getEncoderClass() {
        return ClassifiedAdText.class;
    }
}