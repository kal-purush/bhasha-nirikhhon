/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package at.racemanager.data.store;

import at.racemanager.api.entity.Country;
import static at.racemanager.data.store.StoreTest.em;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author rolhai
 */
public class CountryStoreTest extends StoreTest {

    @Test
    public void getAllCountries() {
        List<Country> entities = em.createQuery("FROM Country", Country.class).getResultList();
        Assert.assertNotNull(entities);
        Assert.assertEquals(7, entities.size());
    }
}