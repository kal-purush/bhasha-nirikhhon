package cooper.finance;

import cooper.finance.FinanceManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class FinanceManagerTest {

    static FinanceManager financeManager;

    @BeforeAll
    static void setUpFinanceManager() {
        financeManager = new FinanceManager();
    }

    @Test
    @Order(1)
    void test_properFirstAdditionToBalanceSheet() {
        int amount = 4000;
        boolean isInflow = true;
        financeManager.addBalance(amount,isInflow);

        Integer expected = 4000;
        ArrayList<Integer> actualList = financeManager.getBalanceSheet();
        Integer actualValue = actualList.get(0);
        assertEquals(expected, actualValue);
    }

    @Test
    @Order(2)
    void test_properSecondAdditionToBalanceSheet() {
        int amount = 5000;
        boolean isInflow = false;
        financeManager.addBalance(amount,isInflow);

        Integer expected = -5000;
        ArrayList<Integer> actualList = financeManager.getBalanceSheet();
        Integer actualValue = actualList.get(1);
        assertEquals(expected, actualValue);
    }

}