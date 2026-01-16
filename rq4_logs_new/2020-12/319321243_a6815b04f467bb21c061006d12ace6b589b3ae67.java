////////////////////////////////////////////////////////////////////
// [STEFANO] [DAL POZ] [1204683]
////////////////////////////////////////////////////////////////////
package it.unipd.tos.business;

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import it.unipd.tos.business.exception.RestaurantBillException;
import it.unipd.tos.model.ItemType;
import it.unipd.tos.model.MenuItem;

public class TakeAwayBillImplementationTest {
    private List<MenuItem> list;
    private TakeAwayBillImplementation test; 
    private User user; 

    @Before
    public void setup() {
        list = new ArrayList<MenuItem>();
        test = new TakeAwayBillImplementation();
        user = new User("Giacomo","Mazzo", 11123, LocalDate.of(2000, 1, 1));
    }

    @Test
    public void testCalcoloTotale(){
        list.add(new MenuItem( ItemType.Budini, "Pinguino",6));
        list.add(new MenuItem( ItemType.Bevande, "Spritz", 3));
        list.add(new MenuItem( ItemType.Gelati, "Latte", 5));
        assertEquals(14, test.getOrderPrice(list,user), 0);
    }

    @Test(expected=RestaurantBillException.class)
    public void testListaVuota() {
        test.getOrderPrice(list, user);
    }

    @Test(expected=RestaurantBillException.class)
    public void testListaNulla() {
        list=null;
        test.getOrderPrice(list, user);
    }
}