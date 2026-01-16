package ar.gov.santafe.meduc.dto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;

/**
 *
 * @author enorrmann
 */
public class CustomDeserializer extends JsonDeserializer<SimpleDto> {

    @Override
    public SimpleDto deserialize(JsonParser jp, DeserializationContext dc) throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        SimpleDto genericDto = new SimpleDto();
        addGenericNode("root", node, genericDto);

        return genericDto;
    }

    private void addGenericNode(String rootField, JsonNode aNode, SimpleDto genericDto) {

        if (aNode.isTextual()) {
            add(rootField, (TextNode) aNode, genericDto);
            return;
        } else if (aNode.isNumber()) {
            add(rootField, (NumericNode) aNode, genericDto);
            return;
        } else if (aNode.isArray()) {
            add(rootField, (ArrayNode) aNode, genericDto);
            return;
        } else {
            Iterator<String> iterator = aNode.getFieldNames();
            while (iterator.hasNext()) {
                String field = iterator.next();
                JsonNode element = aNode.get(field);
                addGenericNode(field, element, genericDto);
            }

        }
    }

    private void add(String field, ArrayNode aNode, SimpleDto genericDto) {
        Iterator<JsonNode> elements = aNode.getElements();
        List list = new ArrayList();
        while (elements.hasNext()) {
            JsonNode element = elements.next();
            if (element.isTextual()) {
                list.add(element.asText());
            } else {
                SimpleDto pp = new SimpleDto();
                addGenericNode(field, element, pp);
                list.add(pp);

            }
        }
        genericDto.add(field, list);

    }

    private void add(String field, NumericNode aNode, SimpleDto genericDto) {
        genericDto.add(field, aNode.asLong());
    }

    private void add(String field, TextNode aNode, SimpleDto genericDto) {
        genericDto.add(field, aNode.asText());
    }
}