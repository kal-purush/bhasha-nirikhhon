package com.marketplace.context.mongo.codec;

import com.marketplace.domain.classifiedad.ClassifiedAdId;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

public class ClassifiedAdIdCodec implements Codec<ClassifiedAdId> {
    public ClassifiedAdIdCodec(CodecRegistry registry) {
    }

    @Override
    public ClassifiedAdId decode(BsonReader reader, DecoderContext decoderContext) {
        String s = reader.readString();
        return ClassifiedAdId.fromString(s);
    }

    @Override
    public void encode(BsonWriter writer, ClassifiedAdId value, EncoderContext encoderContext) {
        writer.writeString(value.id().toString());
    }

    @Override
    public Class<ClassifiedAdId> getEncoderClass() {
        return ClassifiedAdId.class;
    }
}