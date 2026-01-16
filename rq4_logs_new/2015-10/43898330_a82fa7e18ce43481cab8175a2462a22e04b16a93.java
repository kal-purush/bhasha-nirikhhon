package com.nineteen.metadata;

import com.sun.jersey.api.core.HttpContext;
import org.json.JSONObject;
import org.neo4j.server.database.InjectableProvider;

import javax.ws.rs.ext.Provider;
import java.util.HashMap;

/**
 * Created by laeg on 08/10/2015.
 */
@Provider
public class MetadataKeeperProvider extends InjectableProvider<MetadataKeeper> {

    MetadataKeeper metadataKeeper = new MetadataKeeper();

    public MetadataKeeperProvider() {
        super(MetadataKeeper.class);
    }

    @Override
    public MetadataKeeper getValue(HttpContext httpContext) {
        return metadataKeeper;
    }
}