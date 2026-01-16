/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.edu.pk.azu.recyclingwebapp;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.jws.WebService;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.ejb.Stateless;
import javax.xml.ws.WebServiceRef;

/**
 *
 * @author student
 */
@WebService(serviceName = "Recycling")
@Stateless()
public class Recycling {
    @WebServiceRef(wsdlLocation = "WEB-INF/wsdl/localhost_8080/Magazyn/Magazyn.wsdl")
    private pl.edu.pk.azu.magazyn.Magazyn_Service service;

    /**
     * This is a sample web service operation
     */
    @WebMethod(operationName = "hello")
    public String hello(@WebParam(name = "name") String txt) {
        return "Hello " + txt + " !\nHave a nice day!";
    }
    
    @WebMethod(operationName = "handleWithRecycling")
    public void handleWithRecycling() {
        List<Integer> resources = new ArrayList<>();
         // pobieranie listy produktów do recyklingu
        try {
            pl.edu.pk.azu.magazyn.Magazyn port = service.getMagazynPort();
            int stan = 5;//Zepsuty produkt
            
            resources = port.zwrocListeIDProduktow(stan);
            System.out.println("Result = "+ resources);
        } catch (Exception ex) {
            System.out.println("Brak produktów do recyklingu");
        }

        for(int i : resources) {
            //Pobieranie produktu o konkretnym ID
            try { // Call Web Service Operation
                pl.edu.pk.azu.magazyn.Magazyn port = service.getMagazynPort();
                // TODO initialize WS operation arguments here
                int idProjektu = i;
                int stan = 5;
                // TODO process result here
                pl.edu.pk.azu.magazyn.Projekt result = port.wezProdukt(idProjektu, stan);
                System.out.println("Result = "+result);
            } catch (Exception ex) {
                System.out.println("Brak konkretnego produkt do recyklingu");
            }
            
            // Call Web Service Operation
            try { 
                pl.edu.pk.azu.magazyn.Magazyn port = service.getMagazynPort();
                Random generator = new Random();
                int ilosc = generator.nextInt(i);
                port.dodajSurowiec(ilosc);
            } catch (Exception ex) {
                // TODO handle custom exceptions here
            }

            

        }
    }
}