package com.web.crawler.crawling;

import com.web.crawler.model.Page;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class RegexImageCrawlerTestSuite {

    private RegexImageCrawler crawler;

    @Test
    public void shouldFindImages() {

        //Given
        crawler = new RegexImageCrawler();
        Page page = new Page("example.com", "<!doctype html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <title>Example Domain</title>\n" +
                "\n" +
                "    <meta charset=\"utf-8\" />\n" +
                "    <meta http-equiv=\"Content-type\" content=\"text/html; charset=utf-8\" />\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n" +
                "    <style type=\"text/css\">\n" +
                "    body {\n" +
                "        background-color: #f0f0f2;\n" +
                "        margin: 0;\n" +
                "        padding: 0;\n" +
                "        font-family: \"Open Sans\", \"Helvetica Neue\", Helvetica, Arial, sans-serif;\n" +
                "        \n" +
                "    }\n" +
                "    div {\n" +
                "        width: 600px;\n" +
                "        margin: 5em auto;\n" +
                "        padding: 50px;\n" +
                "        background-image: url(\"image.png\");\n" +
                "        border-radius: 1em;\n" +
                "    }\n" +
                "    a:link, a:visited {\n" +
                "        color: #38488f;\n" +
                "        text-decoration: none;\n" +
                "    }\n" +
                "    @media (max-width: 700px) {\n" +
                "        body {\n" +
                "            background-color: #fff;\n" +
                "        }\n" +
                "        div {\n" +
                "            width: auto;\n" +
                "            margin: 0 auto;\n" +
                "            border-radius: 0;\n" +
                "            padding: 1em;\n" +
                "        }\n" +
                "    }\n" +
                "    </style>    \n" +
                "</head>\n" +
                "\n" +
                "<body>\n" +
                "<div>\n" +
                "    <h1>Example Domain</h1>\n" +
                "<img src=\"paris.jpg\" alt=\"Paris\">\n" +
                "    <p>This domain is established to be used for illustrative examples in documents. You may use this\n" +
                        "    domain in examples without prior coordination or asking for permission.</p>\n" +
                        "    <p><a href=\"http://www.iana.org/domains/example\">More information...</a></p>\n" +
                        "</div>\n" +
                        "</body>\n" +
                        "</html>");

        //When
        List<String> list = crawler.find(page);

        //Then
        assertEquals(Arrays.asList("paris.jpg", "image.png"), list);
    }
}