package com.urbanojvr.jsupexample;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        String url = "http://elfreneticoinformatico.com";
        ParserEngine parser = new ParserEngine(url);
        parser.crawl(parser.getBaseUrl());
        System.out.println("Crawler finished. Total URLs: " + parser.getUrlList().size());
    }

}