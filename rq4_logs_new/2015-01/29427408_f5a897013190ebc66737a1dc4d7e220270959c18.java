/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jsoup.html;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Marcos
 */
public class JsoupHTML {

    /**
     * @param args the command line arguments
     */
    private final Document document;

    public JsoupHTML(Document document) {
        this.document = document;
    }

    public static void main(String[] args) {

        try {
            String url = "http://www.tjpi.jus.br/themisconsulta/processo/303743037";
            Document document = Jsoup.connect(url).userAgent("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)").timeout(30000).get();
//            //Elements elements = document.getElementsByClass("content");
//            Elements elements = document.getElementsByClass("div-header");

            JsoupHTML parserProcesso = new JsoupHTML(document);
            parserProcesso.getInfoProcesso();

        } catch (IOException ex) {
            System.out.println("ERROR: Get URL");
        }

    }

    private void getInfoProcesso() {
        Elements elements = document.getElementsByClass("div-table");

        getInfoProcessoDetalhes(elements.eq(0));
        getInfoProcessoPartes(elements.eq(2));
        getInfoProcessoDistribuicoes(elements.eq(4));
        getinfoProcessoMovimentos(elements.eq(5));
    }

    private void getInfoProcessoDetalhes(Elements elements) {
        System.out.println("<<<Detalhes>>>");
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        formatter.setTimeZone(TimeZone.getDefault());
        for (Element element : elements) {
            //String titulo = element.getElementsByClass("table-condensed").text();
            Elements filha = element.getElementsByTag("tr");
            for (Element tb : filha) {
                String tit = tb.getElementsByTag("th").text();
                String desc = tb.getElementsByTag("td").text();
                
                if (tit.equals("Data de Abertura") || tit.equals("")) {
                    try {
                        Date date = formatter.parse(desc);
                        //System.out.println("DATA: " + date);
                        
                        String novaData = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(date);
                        //System.out.println("NOVO FORMATO: " + novaData);
                        desc = novaData;
                    } catch (ParseException ex) {
                        Logger.getLogger(JsoupHTML.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

                System.out.println(tit + ": " + desc);

            }
        }
        System.out.println("------------------------------------------------------------------------------------------------------");
    }

    private void getInfoProcessoPartes(Elements elements) {
        System.out.println("<<<Partes>>>");
        for (Element element : elements) {
            Elements filha = element.getElementsByTag("tr");
            //Jsoup.parse(document.html().replaceAll("(?i)<br[^>]*>", "<pre>\n</pre>")).text();
            for (Element tb : filha) {
                String tit = tb.getElementsByTag("th").text();
                String desc = tb.getElementsByTag("td").text();
                System.out.println(tit + ": " + desc);
            }

        }
        System.out.println("------------------------------------------------------------------------------------------------------");
    }

    private void getInfoProcessoDistribuicoes(Elements elements) {
        System.out.println("<<<Distribuições>>>");
        for (Element element : elements) {
            Elements filha = element.getElementsByTag("tr");
            for (Element tb : filha) {
                String tit = tb.getElementsByTag("th").text();
                String desc = tb.getElementsByTag("td").text();
                System.out.println(tit + ": " + desc);
            }

        }
        System.out.println("------------------------------------------------------------------------------------------------------");
    }

    private void getinfoProcessoMovimentos(Elements elements) {
        int countMov = 0;

        System.out.println("<<<Movimentações>>>");

        for (Element element : elements) {
            Elements filha = element.getElementsByTag("tr");
            for (Element tb : filha) {
                String tit = tb.getElementsByTag("th").text();
                String desc = tb.getElementsByTag("td").text();
                String desc2 = tb.getElementsByTag("td").html();
//                System.out.println(">>>>>>>> "+tb.getElementsByClass("movimentacao-descricao").html());
                String descricao = "";
                descricao = trocarTagBRporQuebraDeLinha(desc2, descricao);

                System.out.println(tit + " --> " + descricao);
                System.out.println("_______________________________________________________________________________________________");
                countMov++;

            }

        }
        System.out.println("Total de Movimentos: " + countMov);
    }

    //Trocar a Tag <BR> por \n (quebra de linha
    public static String trocarTagBRporQuebraDeLinha(String html, String linebreakerString) {
        String result = "";
        if (html.contains(linebreakerString)) {
            result = trocarTagBRporQuebraDeLinha(html, linebreakerString + "1");
        } else {
            result = Jsoup.parse(html.replaceAll("(?i)<br[^>]*>", linebreakerString)).text(); // replace and html line breaks with java linebreak.
            result = result.replaceAll(linebreakerString, "\n");
        }
        return result;
    }

    /**
     * Converte uma String para um objeto Date. Caso a String seja vazia ou
     * nula, retorna null - para facilitar em casos onde formulários podem ter
     * campos de datas vazios.
     *
     * @param data String no formato dd/MM/yyyy a ser formatada
     * @return Date Objeto Date ou null caso receba uma String vazia ou nula
     * @throws Exception Caso a String esteja no formato errado
     */
    public static Date formataData(String data) throws Exception {
        if (data == null || data.equals("")) {
            return null;
        }

        Date date = null;
        try {
            DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
            date = (java.util.Date) formatter.parse(data);
        } catch (ParseException e) {
            throw e;
        }
        return date;
    }
}