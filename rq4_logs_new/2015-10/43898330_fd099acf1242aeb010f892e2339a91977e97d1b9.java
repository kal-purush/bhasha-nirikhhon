package com.nineteen.metadata;

import org.json.JSONObject;

import javax.ws.rs.ext.Provider;
import java.util.HashMap;

/**
 * Created by laeg on 08/10/2015.
 */
@Provider
public class MetadataKeeper {

    public HashMap<String, JSONObject> metaCollection;


    public MetadataKeeper()
    {
        metaCollection = new HashMap<>();
    }

    public void saveValue(String nodeLabel, JSONObject jObj)
    {
        System.out.println("SAVE VALUE");
        metaCollection.put(nodeLabel, jObj);
    }

    public JSONObject returnValue(String nodeLabel)
    {

        System.out.println("RETURN VALUE");
        if(metaCollection.containsKey(nodeLabel))
        {
            System.out.println("RETURNED VALUE");
            return metaCollection.get(nodeLabel);
        }
        else
        {
            throw new NullPointerException();
        }
    }
}