/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bank;

import model.Bank;
import javax.jws.WebService;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.ejb.Stateless;

/**
 *
 * @author syst3m
 */
@WebService(serviceName = "BankServices")
@Stateless()
public class BankServices {

    /**
     * This is a sample web service operation
     */
    @WebMethod(operationName = "pay")
    public boolean pay(@WebParam(name = "personName") String personName,@WebParam(name = "creditCard") String creditCard) {
        Bank bank = new Bank();
        return bank.pay(creditCard, personName);
    }
}