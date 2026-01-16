/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fi.hoski.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author tkv
 */
public class BankAccountNumberTest
{
    
    public BankAccountNumberTest()
    {
    }

    @Test
    public void test1()
    {
            BankAccountNumber ptn = new BankAccountNumber("FI4250001510000023");
            assertEquals("50001510000023", ptn.toBankingBarcodeString());
            ptn = new BankAccountNumber("123456-785");
            assertEquals("12345600000785", ptn.toBankingBarcodeString());
            ptn = new BankAccountNumber("423456-781");
            assertEquals("42345670000081", ptn.toBankingBarcodeString());
            ptn = new BankAccountNumber("FI39 6601 0010 2435 25");
            assertEquals("FI39 6601 0010 2435 25", ptn.getIBAN());
    }
    
}