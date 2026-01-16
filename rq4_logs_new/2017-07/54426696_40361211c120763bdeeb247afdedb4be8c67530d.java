package io.citrine.jcc.search.dataset.query;

import io.citrine.jpif.util.PifObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link DatasetReturningQuery}.
 *
 * @author Kyle Michel
 */
public class DatasetReturningQueryTest {

    /**
     * Test backwards compatibility in deserializing DatasetQuery objects from v1.x to {@link DatasetReturningQuery}
     * objects.
     *
     * @throws IOException if thrown in this test.
     */
    @Test
    public void testDatasetQueryDeserialization() throws IOException {

        // Deserialize a PifQuery object into a PifSystemReturningQuery object
        final DatasetReturningQuery datasetReturningQuery = PifObjectMapper.getInstance().readValue(
                this.getClass().getClassLoader().getResourceAsStream("datasetQuery.json"), DatasetReturningQuery.class);

        // Check that all values were parsed
        Assert.assertEquals(10, (long) datasetReturningQuery.getFrom());
        Assert.assertEquals(10, (long) datasetReturningQuery.getFromIndex());
        Assert.assertEquals(100, (long) datasetReturningQuery.getSize());
        Assert.assertTrue(datasetReturningQuery.getRandomResults());
        Assert.assertEquals(0, (int) datasetReturningQuery.getRandomSeed());
        Assert.assertTrue(datasetReturningQuery.getScoreRelevance());
        Assert.assertTrue(datasetReturningQuery.getCountPifs());
        Assert.assertEquals(1, datasetReturningQuery.queryLength());
        Assert.assertEquals(2, datasetReturningQuery.getQuery(0).systemLength());
        Assert.assertEquals("lithium",
                datasetReturningQuery.getQuery(0).getSystem(0).getNames(0).getFilter(0).getEqual());
        Assert.assertEquals("Li",
                datasetReturningQuery.getQuery(0).getSystem(1).getChemicalFormula(0).getFilter(0).getEqual());
    }
}