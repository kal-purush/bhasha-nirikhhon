package org.talkshowhost;

import javax.net.ssl.SSLServerSocketFactory;
import java.util.*;


public class Ciphers {

    public static void main(String[] args) throws Exception {
        SSLServerSocketFactory fac = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();

        ArrayList<String> avail = new ArrayList<String>();
        avail.addAll(Arrays.asList(fac.getSupportedCipherSuites()));

        Collections.sort(avail);

        HashSet<String> def = new HashSet<String>();

        def.addAll(Arrays.asList(fac.getDefaultCipherSuites()));


        for(String cipher: avail){
            System.out.format("%40s%16s\n", cipher, def.contains(cipher));

        }


    }
}