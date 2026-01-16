package ecs189.querying;

import java.io.IOException;

/**
 * Created by Vincent on 10/1/2017.
 */
public interface Querier {
    String query(String ID) throws IOException;
}