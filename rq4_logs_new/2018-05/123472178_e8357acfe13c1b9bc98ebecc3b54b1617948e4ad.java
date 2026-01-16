package com.web.crawler.link.replacer.replacement.absolute.address;

import com.web.crawler.download.namegenerator.NameGenerator;
import com.web.crawler.link.replacer.replacement.Replacement;
import com.web.crawler.model.CrawledLink;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbsoluteAddress implements Replacement {
//TODO this is POC , need to find better solution for this case
    private static final String ABSOLUTE_REGEX = "(http(s)?://)?(www\\.)?([-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*))";

    @Override
    public String replace(CrawledLink crawledLink, String address) {

        String link = crawledLink.getCrawledLink();
//TODO zapytać o takie rozwiązanie
        Matcher m = Pattern.compile(NameGenerator.GENERATE_NAME_REGEX).matcher(link);
        m.find();
        link = m.group(0);
        link = link.replaceAll("www\\.", "");

        return crawledLink.getHead() + evaluateDepth(address) + link + crawledLink.getTail();
    }

    @Override
    public boolean supports(String link) {

        Matcher m = Pattern.compile(ABSOLUTE_REGEX).matcher(link);

        if (m.matches()) {
            return true;
        }
        return false;
    }

    private String evaluateDepth(String address) {

        String result = "";
        int count = ((int) address.chars()
                .filter(n -> n == '/')
                .count()) - 1;
        for (int i = 0; i < count; i++) {
            result += "../";
        }

        return result;
    }
}