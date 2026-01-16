package DP_entity_linking;

import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;

/**
 * Created by miroslav.kudlac on 10/26/2015.
 */
public interface IEvaluation {

    /**
     * Ohodnot ci sa v dokumentoch nachadza odpoved.
     *
     * @param hits
     * @param answer
     * @return skore od 0-1, 1 je najvacsie skore, 0 ked sa nenaslo
     * @throws IOException
     */
    float getScore(ScoreDoc[] hits, String answer) throws IOException;
}