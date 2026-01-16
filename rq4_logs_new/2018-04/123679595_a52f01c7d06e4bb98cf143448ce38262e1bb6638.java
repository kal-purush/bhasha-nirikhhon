package data;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class UserParser extends StdDeserializer<UserLogin>{
	public UserParser() {
        this(null);
    }
	protected UserParser(Class<?> vc) {
		super(vc);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public UserLogin deserialize(JsonParser parser, DeserializationContext deserializer) {
		UserLogin user = new UserLogin();
		
        ObjectCodec codec = parser.getCodec();
        JsonNode node;
		try {
			node = codec.readTree(parser);
			
			JsonNode idNode = node.get("id");
	        user.setId(idNode.asInt());
	        
	        JsonNode usernameNode = node.get("username");
	        user.setUsername(usernameNode.asText());
	        
	        JsonNode pswdNode = node.get("password");
	        user.setPassword(pswdNode.asText());
	        
	        return user;
		} catch (IOException e) {
			e.printStackTrace();
		}
         return null;
	}
}