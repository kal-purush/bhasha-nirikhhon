/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bank;

import javax.ejb.Stateless;
import javax.jws.WebService;
import model.Bank;

/**
 *
 * @author Alex
 */
@WebService(serviceName = "BankServicesService", portName = "BankServicesPort", endpointInterface = "bank.BankServices", targetNamespace = "http://bank/", wsdlLocation = "WEB-INF/wsdl/BankWS/bankWSDLWrapper.wsdl")
@Stateless
public class BankWS {

    public boolean pay(java.lang.String personName, java.lang.String creditCard, double amount) {
        Bank bank = new Bank();
        return bank.pay(creditCard, personName, amount);
    }

}