package org.bund;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import java.math.BigDecimal;

public class FieldCaster {

    // the orderbook directory contains various security reference data
    // that can by dynamically updated throughout a trading day (really?)
    // NOTE: currently we only use number of price digits in order to cast
    //       price information
    Map<Integer,Integer> obDir;
    // system time variable
    int secSinceMidnight;

    // constructor
    public FieldCaster() {
        obDir=new HashMap<Integer,Integer>();
        secSinceMidnight=0;
    }

    // type cast the fields of the message
    public ArrayList<String> cast(ArrayList<String> msg) {

            // for all, do timestamp parsing
            // (convert timestamp to seconds since midnight)
            msg.set(1, String.valueOf((new BigDecimal(new BigInteger(msg.get(1)),6)).add(new BigDecimal(secSinceMidnight))));

            // for some, do additional conversion
            switch(msg.get(0)) {
                case "R": // orderbook directory message
                    // -> update the orderbook directory
                    obDir.put(Integer.parseInt(msg.get(2)),Integer.parseInt(msg.get(10)));
                    break;
                case "T": // Time Stamp – Seconds message
                    // -> update current number of seconds since midnight
                    secSinceMidnight = Integer.parseInt(msg.get(1));
                case "A": // add order message
                    // remove placeholder for non-available prices
                    if(msg.get(6).equals("2147483647")) {
                        msg.set(6,null);
                        break;
                    }
                    // -> add decimal point from orderbook directory
                    msg.set(6, (new BigDecimal(new BigInteger(msg.get(6)), obDir.get(msg.get(5)))).toString());
                    break;
                case "C": // order executed with price message
                    // -> add decimal point from orderbook directory
                    // TODO: have no orderbood ID only order id
                    break;
                case "P": // trade message
                    // -> add decimal point from orderbook directory
                    msg.set(4,(new BigDecimal(new BigInteger(msg.get(4)),obDir.get(msg.get(2)))).toString());
                    break;
                case "U": // order replace message
                    // -> add decimal point from orderbook directory
                    // TODO: have no orderbood ID only order id
                    break;
                case "I": // Indicative Price / Quantity message
                    // -> add decimal point from orderbook directory
                    msg.set(4,(new BigDecimal(new BigInteger(msg.get(4)),obDir.get(msg.get(3)))).toString());
                    msg.set(5,(new BigDecimal(new BigInteger(msg.get(5)),obDir.get(msg.get(3)))).toString());
                    msg.set(6,(new BigDecimal(new BigInteger(msg.get(6)),obDir.get(msg.get(3)))).toString());
                    break;
                default:
                    // nothing else to do here
            }

        // return casted field
        return(msg);
    }

}