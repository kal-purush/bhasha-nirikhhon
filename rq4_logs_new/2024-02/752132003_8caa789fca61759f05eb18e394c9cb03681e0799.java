package com.web.Crawler;

import java.io.IOException;
import org.springframework.stereotype.Component;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


@Component
public class WebCrawler {

    public void crawl() {
        String url = "https://www.youthcenter.go.kr/youngPlcyUnif/youngPlcyUnifDtl.do?pageIndex=1&frameYn=&bizId=R2024010218403&dtlOpenYn=&plcyTpOpenTy=&plcyCmprInfo=&srchWord=&chargerOrgCdAll=&srchAge=&trgtJynEmp=&trgtJynEmp=&srchSortOrder=1&pageUnit=60";

        try {
            Document document = Jsoup.connect(url).get();

            // 필요한 정보 추출
            String title = document.title();
            System.out.println("Title: " + title);

            // 원하는 선택자를 사용하여 원하는 요소 추출
            Elements elements = document.select(".common_table01");
            for (Element element : elements) {
                System.out.println(element.text());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}