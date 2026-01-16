package Transport4Future.TokenManagement.model.deserializers;

import Transport4Future.TokenManagement.model.Token;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public class TokenDeserializer extends TypeAdapter<Token> {

    @Override
    public Token read(JsonReader reader) throws IOException {
        System.out.println(reader.toString());
        return null;
    }

    @Override
    public void write(JsonWriter writer, Token token) throws IOException {
    }
}