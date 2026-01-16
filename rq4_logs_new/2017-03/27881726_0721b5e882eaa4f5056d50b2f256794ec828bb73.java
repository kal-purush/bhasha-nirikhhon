/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.shareok.data.sagedata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.shareok.data.config.DataUtil;
import org.shareok.data.config.ShareokdataManager;
import org.shareok.data.datahandlers.DataHandlersUtil;
import org.shareok.data.documentProcessor.DocumentProcessorUtil;

/**
 *
 * @author Tao Zhao
 */
public class SageJournalIssueDateProcessor {
    
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SageJournalIssueDateProcessor.class);
    
    private static final String[] unparseableJournals = {"Journal of Rehabilitation and Assistive Technologies Engineering"};
    
    // These journals ONLY have a link to PDF full text and should used the first date of the month as the pub date of the issue
    private static List<String> NO_ARTICLE_PUB_DATE_JOURNALS_LIST = new ArrayList<>();
    static{
        NO_ARTICLE_PUB_DATE_JOURNALS_LIST.add("Plastic Surgery Case Studies");
        NO_ARTICLE_PUB_DATE_JOURNALS_LIST.add("Plastic Surgery");
    }
    
    @SuppressWarnings("empty-statement")
    public void retrieveSageJournalVolIssueDates(Map<String, String>processedJournalsMap){
        List<String> processedJournals = new ArrayList<>();
//        JSONObject jsonObj = getSavedSageJournalVolIssueDateInformation();
        try{
            Map<String, Map<String, String>>journalMap = getSavedSageJournalVolIssueDateInformation();
            if(null == journalMap){
                journalMap = new HashMap<>();
            }
            Document doc = null;
            try {
                doc = Jsoup.connect("http://journals.sagepub.com/action/showPublications?pageSize=20&startPage=199")
                        .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
                        .cookie("auth", "token")
                        .timeout(300000)
                        .get();
                Elements trs = doc.select("form#browsePublicationsForm").get(0).select("table").get(0).select("tbody").get(0).select("tr");
                for(Element tr : trs){
                    Element link = tr.select("td").get(1).select("a").get(0);
                    String journalName = link.text();
                    String journalLink = SageDataUtil.SAGE_HTTP_PREFIX + link.attr("href");
                    String[] linkInfo = journalLink.split("/");
                    String journalIssuesLink = SageDataUtil.SAGE_HTTP_PREFIX + "/loi/" + linkInfo[linkInfo.length-1];
                    if(null == journalMap.get(journalName)){
                        Map<String, String> infoMap = new HashMap<>();
                        infoMap.put("homeLink", journalLink);
                        infoMap.put("issueLink", journalIssuesLink);
                        journalMap.put(journalName, infoMap);
                    }
                    else{
                        Map<String, String> infoMap = journalMap.get(journalName);
                        if(null == infoMap.get("homeLink")){
                            infoMap.put("homeLink", journalLink);
                        }
                        if(null == infoMap.get("issueLink")){
                            infoMap.put("issueLink", journalIssuesLink);
                        }
                    }
                }
                int kk = 0;
                mainLoop:
                for(String journal : journalMap.keySet()){
                    System.out.println("Print out journal "+journal+" information :");
                    if(null != processedJournalsMap && (journal == null ? processedJournalsMap.get(journal) == null : journal.equals(processedJournalsMap.get(journal)))){
                        System.out.println("Journal : has already been processed!");
                        continue;
                    }
//                    if(journal.contains("Christian Education")){
//                        System.out.println("Journal name : International Journal of Health Services, cannot be processed!");
////                        continue;
//                    }
//                    if(journal.contains("Plastic Surgery")){
//                        System.out.println("Journal name : International Journal of Health Services, cannot be processed!");
//                        continue;
//                    }
                    if(!journal.contains("Policy Futures in Education")){
                        System.out.println("Journal name : "+journal);
                        continue;
                    }
                    Map<String, String> journalInfoMap = journalMap.get(journal);
                    for(String key : journalInfoMap.keySet()){
                        if(key.equals("issueLink")){
                            doc = Jsoup.connect(journalInfoMap.get(key))
                                    .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
                                    .cookie("auth", "token")
                                    .timeout(300000)
                                    .get();
                            Thread.sleep(2000);
                            if(null != doc){
                                Map<String, Map<String, String>> dataMap;
                                if(null != journalMap.get(journal).get("data")){
                                    dataMap = DataUtil.getMapFromJson(journalMap.get(journal).get("data"));
                                }
                                else{
                                    dataMap = new HashMap<>();
                                }
                                Elements decaseDivs = doc.select("div.decade");
                                if(null != decaseDivs && decaseDivs.size() > 0){
                                    for(Element decade : decaseDivs){
                                        Element yearsDiv = decade.select("div.years").get(0);
                                        Elements volumesDiv = yearsDiv.select("div .volumes, .expandedDiv");
                                        if(null != volumesDiv && volumesDiv.size() > 0){
                                            for(Element volumesDivElement : volumesDiv){
                                                String volume = volumesDivElement.select("a.expander, .open").get(0).text().trim().split("Volume")[1].trim();
                                                Elements issueInfoDivEles = volumesDivElement.select("div.js_issue");
                                                if(null != issueInfoDivEles && issueInfoDivEles.size() > 0){
                                                    Map<String, String> issueMap = new HashMap<>();
                                                    for(Element issueInfoDiv : issueInfoDivEles){
                                                        String issueText = issueInfoDiv.select("a").get(0).text();
                                                        issueText = issueText.split(", ")[0].split("Issue")[1].trim();
                                                        String oldIssueDate = "";
                                                        String issueDate = "";
                                                        if(NO_ARTICLE_PUB_DATE_JOURNALS_LIST.contains(journal)){
                                                            issueDate = "01 " + issueInfoDiv.select("span.loiIssueCoverDateText").get(0).text().trim();
                                                            oldIssueDate = issueDate;
//                                                            if(issueDate.contains("Winter")){
//                                                                issueDate = issueDate.replaceAll("Winter", "October");
//                                                            }
//                                                            if(issueDate.contains("Fall") || issueDate.contains("Autumn")){
//                                                                issueDate = issueDate.replaceAll("Fall", "September");
//                                                                issueDate = issueDate.replaceAll("Autumn", "September");
//                                                            }
//                                                            if(issueDate.contains("Summer")){
//                                                                issueDate = issueDate.replaceAll("Summer", "April");
//                                                            }
//                                                            if(issueDate.contains("Spring")){
//                                                                issueDate = issueDate.replaceAll("Spring", "January");
//                                                            }
//                                                            try{                                                            
//                                                                // for date string like "01 July-October 2016"
//                                                                if(issueDate.contains("-")){
//                                                                    String[] dateInfo = issueDate.split("-");
//                                                                    issueDate = dateInfo[0] + " " + dateInfo[1].split(" ")[1];
//                                                                }
//                                                                // for date string like "01 July/October 2016"
//                                                                if(issueDate.contains("/")){
//                                                                    String[] dataInfo = issueDate.split("/");
//                                                                    issueDate = dataInfo[0] + " " + dataInfo[1].split(" ")[1];
//                                                                }
//                                                            }
//                                                            catch(ArrayIndexOutOfBoundsException ex){
//                                                                System.out.println("Journal name: "+journal);
//                                                                System.out.println("Volume: "+volume+", issue: "+issueText);
//                                                                System.out.println("This date string cannot be parsed: "+oldIssueDate);
//                                                                ex.printStackTrace();
//                                                                continue;
//                                                            }
                                                            try{
                                                                issueDate = DataHandlersUtil.convertFullMonthDateStringFormat(issueDate);
                                                            }
                                                            catch(ParseException ex){
//                                                                if(!journal.contains("OMEGA - Journal of Death and Dying")){
//                                                                    continue;
//                                                                }
                                                                System.out.println("Journal name: "+journal);
                                                                System.out.println("Volume: "+volume+", issue: "+issueText);
                                                                System.out.println("This date string cannot be parsed: "+issueDate);
                                                                ex.printStackTrace();                                                            
                                                            }
                                                            
                                                        }
                                                        else{
                                                            try{
                                                                Element issueLinkEle = issueInfoDiv.select("a").get(0);
                                                                String issueLink = issueLinkEle.attr("href");
                                                                Thread.sleep(2000);
                                                                doc = Jsoup.connect(issueLink)
                                                                        .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
                                                                        .cookie("auth", "token")
                                                                        .timeout(300000)
                                                                        .get();
                                                                Elements articleDivs = doc.select("div.art_title, .linkable");
                                                                String articleLink = SageDataUtil.SAGE_HTTP_PREFIX + articleDivs.get(0).select("a.ref, .nowrap").get(0).attr("href");
                                                                if(articleLink.contains("pdf/")){
                                                                    System.out.println("journal: "+journal+" volume="+volume+" issue="+issueText+" has ONLY PDF links!");
                                                                    continue;
                                                                }
                                                                Thread.sleep(2000);
                                                                doc = Jsoup.connect(articleLink)
                                                                        .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36")
                                                                        .cookie("auth", "token")
                                                                        .timeout(300000)
                                                                        .get();
                                                                Element pubDateDiv = doc.select("div.published-dates").get(0);
                                                                issueDate = pubDateDiv.text().split("Issue published:")[1].trim();
                                                                oldIssueDate = issueDate;
                                                                issueDate = DataHandlersUtil.convertFullMonthDateStringFormat(issueDate);
                                                            }
                                                            catch(Exception ex){
                                                                logger.error("Cannot get the issue date for journal ="+journal+" volume="+volume+" issue="+issueText+" date="+oldIssueDate, ex);
                                                            }
                                                        }
                                                        if(DataHandlersUtil.datesCompare(issueDate, "2000-01-01") < 0){
                                                            if(dataMap.size() > 0){
                                                                ObjectMapper mapper = new ObjectMapper();
                                                                String json = mapper.writeValueAsString(dataMap);
                                                                journalInfoMap.put("data", json);
                                                            }
                                                            processedJournals.add(journal);
                                                            continue mainLoop;
                                                        }
                                                        try{
                                                            if(null != dataMap && dataMap.size() > 0 && null != dataMap.get(volume) && null != dataMap.get(volume).get(issueText)){
                                                                continue;
                                                            }
                                                            else{
                                                                issueMap.put(issueText, issueDate);
                                                                dataMap.put(volume, issueMap);
                                                                System.out.println("This is vol. "+volume+" and issue "+issueText+" and date "+issueDate);
                                                            }
                                                        }
                                                        catch(Exception ex){

                                                        }                                                       
                                                    }                                                    
                                                }
                                                
                                            }
                                        }
                                    }
                                }
                                if(dataMap.size() > 0){
                                    ObjectMapper mapper = new ObjectMapper();
                                    String json = mapper.writeValueAsString(dataMap);
                                    journalInfoMap.put("data", json);
                                }
                            }
                            
                        }
                    }
                    processedJournals.add(journal);
                    if(kk > 80){
                        break;
                    }
                    kk++;
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(journalMap);
            String sageJournalIssueDateInfoFilePath = ShareokdataManager.getSageJournalIssueDateInfoFilePath();
            File sageFile = new File(sageJournalIssueDateInfoFilePath);
            if(sageFile.exists()){
                String sageJournalIssueDateInfoFilePathOld = sageJournalIssueDateInfoFilePath.split("\\.")[0] + "_" + DataHandlersUtil.getCurrentTimeString() + ".json";
                sageFile.renameTo(new File(sageJournalIssueDateInfoFilePathOld));
            }
            DocumentProcessorUtil.outputStringToFile(json, ShareokdataManager.getSageJournalIssueDateInfoFilePath());
            System.out.println("processed journals = "+mapper.writeValueAsString(processedJournals));
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Map<String, Map<String, String>> getSavedSageJournalVolIssueDateInformation(){
        try {
            Map<String, Map<String, String>> map = new HashMap<>();
            String sageJournalIssueDateInfoFilePath = ShareokdataManager.getSageJournalIssueDateInfoFilePath();
            File f = new File(sageJournalIssueDateInfoFilePath);
            if (f.exists()){
                InputStream is = new FileInputStream(sageJournalIssueDateInfoFilePath);
                String jsonTxt = IOUtils.toString(is);
                JSONObject json = new JSONObject(jsonTxt);       
                return DataUtil.getMapOfStringMapFromJsonObject(json);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(SageJournalIssueDateProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    public Map<String, String> getIssueDateProcessedSageJournals(Map<String, Map<String, String>> sageJournalVolIssueDateData){
        Map<String, String> processedJournals = new HashMap<>();
        for(String journal : sageJournalVolIssueDateData.keySet()){
            Map<String, String> journalData = sageJournalVolIssueDateData.get(journal);
            if(null != journalData && null != journalData.get("data")){
                processedJournals.put(journal, journal);
            }
        }
        return processedJournals;
    }
    
    /**
     * Sage journal "OMEGA - Journal of Death and Dying" has some date information cannot be parsed: e.g.  01 January 2003-2004
     * @param journalVolumeIssueData : json object loaded from sageJournalIssueDateInfo.json 
     * @param volume : volume string
     * @param issue : issue string
     * @return : a string map of start and end dates
     */
    public Map<String, String> getStartEndDatesForOMEGAJournalOfDeathAndDying(JSONObject journalVolumeIssueData, String volume, String issue){
        Map<String, String> datesMap = new HashMap<>();
        try{
            String issueDateStr = journalVolumeIssueData.getJSONObject("OMEGA - Journal of Death and Dying").getJSONObject("data").getJSONObject(volume).getString(issue);
            String[] issueDateStrInfo = issueDateStr.split("-");
            String[] info = issueDateStrInfo[0].split(" ");
            String endDate = info[0] + " " + info[1] + " " + issueDateStrInfo[1];
            datesMap.put("startDate", issueDateStrInfo[0]);
            datesMap.put("endDate", endDate);
            return datesMap;
        }
        catch(Exception ex){
            logger.error("Cannot parse the date information of journal : OMEGA - Journal of Death and Dying", ex);
        }
        return null;
    }
}