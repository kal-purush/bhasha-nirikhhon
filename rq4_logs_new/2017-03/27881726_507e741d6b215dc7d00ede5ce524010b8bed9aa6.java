/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareok.data.sagedata;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.shareok.data.datahandlers.DataHandlersUtil;
import org.shareok.data.documentProcessor.DocumentProcessorUtil;
import org.shareok.data.htmlrequest.HtmlParser;
import org.shareok.data.htmlrequest.HttpRequestHandler;
import org.shareok.data.htmlrequest.exceptions.ErrorHandlingResponseException;
import org.shareok.data.htmlrequest.exceptions.ErrorResponseCodeException;
import org.shareok.data.sagedata.exceptions.EmptyArticleTypeInfoException;
import org.shareok.data.sagedata.exceptions.NoSageSearchCurrentArticleIndexException;
import org.shareok.data.sagedata.exceptions.NoSageSearchTotalRecordsException;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author Tao Zhao
 */
public class SageApiDataImpl implements SageApiData {
    
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SageApiDataImpl.class);
    
    private HttpRequestHandler reqHandler;
    private HtmlParser parser;

    @Autowired
    public void setReqHandler(HttpRequestHandler reqHandler) {
        this.reqHandler = reqHandler;
    }

    @Autowired
    public void setParser(HtmlParser parser) {
        this.parser = parser;
    }
    
    @Override
    public String getApiResponseByDatesAffiliate(String startDate, String endDate, String affiliate) {
        String json = null;
        String response = null;
        String query = getApiQuery(startDate, endDate, affiliate);
        System.out.println("query = " + query);
        List<Map<String, String>>articleList = new ArrayList<>();
        try {
            Document doc = getApiResponseByQuery(query);
            int total = getTotalArticlesFound(doc);
            int currentIndex = getCurrentArticleIndex(doc);
            getArticleInfoFromDoc(articleList, doc);
            int count = 1;
            for(Map<String, String> articleInfo : articleList){
                System.out.println("This is No "+String.valueOf(count) + " article");
                for(String key : articleInfo.keySet()){
                    System.out.println(key + " = " + articleInfo.get(key));
                }
                count++;
            }
//            Map<String, ArrayList<String>> data = parseSageSearchResponse(response);
//            System.out.println("data size = "+String.valueOf(data.size()));
        } catch (ErrorResponseCodeException | ErrorHandlingResponseException ex) {
            logger.error("Cannot get response sage data after http get request wiht query = "+query, ex);
            return null;
        } catch (NoSageSearchTotalRecordsException ex) {
            logger.error("Cannot get total records!", ex);
        } catch (NoSageSearchCurrentArticleIndexException ex) {
            logger.error("Cannot get index of current article!", ex);
        }
        
        return json;
    }

    @Override
    public String outputResponse() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    private String getApiQuery(String startDate, String endDate, String affiliate){
        
        try {
            String startYear = DataHandlersUtil.getYearFromSimpleDateString(startDate);
            String endYear = DataHandlersUtil.getYearFromSimpleDateString(endDate);
            
            return SageDataUtil.API_SEARCH_PREFIX + "field1=Affiliation&text1=" + URLEncoder.encode(affiliate, "UTF-8")+ "&field2=AllField&text2=&Ppub=&Ppub=&AfterYear=" + startYear + "&BeforeYear=" + endYear + "&access=&pageSize=20&startPage=0&";
        } catch (Exception ex) {
            logger.error("Cannot encode the query parameters!", ex);
        }
        return null;
    }
    
    private Document getApiResponseByQuery(String query) throws ErrorResponseCodeException, ErrorHandlingResponseException{

        Document doc = null;
        try {
            doc = Jsoup.connect("http://journals.sagepub.com/action/doSearch?field1=Affiliation&text1=University+of+Oklahoma&AfterYear=2017&BeforeYear=2017&access=&")
                    .data("query", "Java")
                    .userAgent("Mozilla")
                    .cookie("auth", "token")
                    .timeout(3000)
                    .get();
        } catch (IOException ex) {
            logger.error("Cannot get the SAGE pub search results by the query: "+query, ex);
        }
        return doc;
    }
    
    private Map<String,ArrayList<String>> parseSageSearchResponse(String response){
//        Element paginationDiv = parser.
        Map<String,ArrayList<String>> data = parser.metaDataParser(response);
        return data;
    }
    
    private int getTotalArticlesFound(Document doc) throws NoSageSearchTotalRecordsException{
        int total = -1;
        Elements eles = doc.select("span.emphasis");
        if(null != eles && eles.size() > 0){
            Element emphasisSpan = eles.get(0);
            Element emphasisSpanParent = emphasisSpan.parent();
            String totalStr = emphasisSpanParent.text().trim().split(" of ")[1];
            total = Integer.valueOf(totalStr);
        }
        else{
            throw new NoSageSearchTotalRecordsException("Cannot find the total records information!");
        }
        return total;
    }
    
    private int getCurrentArticleIndex(Document doc) throws NoSageSearchCurrentArticleIndexException{
        int index = -1;
        Elements eles = doc.select("span.emphasis");
        if(null != eles && eles.size() > 0){
            Element emphasisSpan = eles.get(0);
            Element emphasisSpanParent = emphasisSpan.parent();
            String indexStr = (emphasisSpanParent.text().trim().split(" of ")[0]).split(" – ")[1];
            index = Integer.valueOf(indexStr);
        }
        else{
            throw new NoSageSearchCurrentArticleIndexException("Cannot find the total records information!");
        }
        return index;
    }
    
    private void getArticleInfoFromDoc(List<Map<String, String>> articleList, Document doc){
        Elements articleElements = doc.select("article.searchResultItem");
        if(articleElements.size() > 0){
            for(Element article : articleElements){ 
                try{
                    String articleType = article.select("span.ArticleType").get(0).text();
                    if(null == articleType || DocumentProcessorUtil.isEmptyString(articleType)){
                        try {
                            throw new EmptyArticleTypeInfoException("Cannot get article type information!");
                        } catch (EmptyArticleTypeInfoException ex) {
                            logger.error(ex);
                            continue;
                        }
                    }
                    boolean fullAccess = false;
                    Elements accessIcons = article.select("img.freeAccess");
                    if(null != accessIcons && accessIcons.size() > 0){
                        Element accessIconElement = accessIcons.get(0);
                        if(accessIconElement.attr("title").equals("Free Access")){
                            fullAccess = true;
                        }
                    }
                    else{
                        accessIcons = article.select("img.fullAccess");
                        if(null != accessIcons && accessIcons.size() > 0){
                            Element accessIconElement = accessIcons.get(0);
                            if(accessIconElement.attr("title").equals("Full Access")){
                                fullAccess = true;
                            }
                        }
                    }
                    if(fullAccess == true){
                        Map articleInfoMap = new HashMap<>();
                        Element titleDiv = article.select("div.art_title").get(0);
                        Element titleLink = titleDiv.select("a.ref").get(0);
                        articleInfoMap.put("title", titleLink.text().trim());

                        Element tocDeliverFormatsLinks = article.select("div.tocDeliverFormatsLinks").get(0);
                        Element absLink = tocDeliverFormatsLinks.select("a.abstract").get(0);
                        String abstractLink = SageDataUtil.SAGE_HTTP_PREFIX + absLink.attr("href").trim();
                        articleInfoMap.put("abstractLink", abstractLink);

                        Element pdfLink = tocDeliverFormatsLinks.select("a.pdf").get(0);
                        String pdfLinkStr = SageDataUtil.SAGE_HTTP_PREFIX + pdfLink.attr("href").trim();
                        articleInfoMap.put("pdfLink", pdfLinkStr);

                        Element pubDate = article.select("span.maintextleft").get(0);
                        String pubDateStr = pubDate.text().split("Published ")[1].trim().split("\\.")[0];
                        articleInfoMap.put("pubDate", pubDateStr);

                        articleList.add(articleInfoMap);
                    }      
                }
                catch(Exception ex){
                    logger.error("Cannot interpret the SAGE article information!", ex);
                }
            }
        }
    }
}