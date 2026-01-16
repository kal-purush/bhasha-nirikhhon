package Transport4Future.TokenManagement.service;

import Transport4Future.TokenManagement.config.Constants;
import Transport4Future.TokenManagement.model.Token;
import Transport4Future.TokenManagement.model.TokenRequest;
import Transport4Future.TokenManagement.model.typeadapter.TokenRequestTypeAdapter;
import Transport4Future.TokenManagement.model.typeadapter.TokenTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;

/**
 * This class manages the IO on Json object - text.
 */
public class JsonManager {
    /**
     * Prepares the Gson class with our customs type adapters.
     *
     * @return Gson object created with the builder.
     */
    private Gson getDeserializer() {
        GsonBuilder builder = new GsonBuilder();
        if (Constants.IS_DEV) {
            builder.setPrettyPrinting();
        }
        builder.create();
        builder.registerTypeAdapter(Token.class, new TokenTypeAdapter());
        builder.registerTypeAdapter(TokenRequest.class, new TokenRequestTypeAdapter());
        return builder.create();
    }

    /**
     * Encode a object to json.
     *
     * @param obj The object to encode.
     * @param <T> The TypeToken reflection class that is the object class.
     * @return JSON string.
     */
    public <T> String encode(T obj) {
        return this.getDeserializer().toJson(
                obj
        );
    }

    /**
     * Decode json to object.
     *
     * @param json             JSON string.
     * @param deserializeClass Class to reflect.
     * @param <T>              The TypeToken reflection class that is the object class.
     * @return Reflected object.
     */
    public <T> T decode(String json, Class<T> deserializeClass) {
        return this.getDeserializer().fromJson(json, deserializeClass);
    }

    /**
     * Decode json to object.
     *
     * @param json JSON string.
     * @param type Type class to reflect.
     * @param <T>  The TypeToken reflection class that is the object class.
     * @return Reflected object.
     */
    public <T> T decode(String json, Type type) {
        return this.getDeserializer().fromJson(json, type);
    }
}