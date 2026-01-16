package com.github.ssullivan.core;

import com.github.ssullivan.model.crawler.RobotRules;
import com.github.ssullivan.model.crawler.RobotsTxt;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RobotsTxtParser {
  private static final Pattern UserAgentPattern = Pattern.compile("^[Uu]ser-[Aa]gent:\\s+(.*?)\\s*(?:#.*)?$");
  private static final Pattern DisAllowRulePattern = Pattern.compile("^[Dd]isallow:\\s+(.*?)\\s+(?:#.*)?$");
  private static final Pattern AllowRulePattern = Pattern.compile("^[Aa]llow:\\s+(.*?)\\s*(?:#.*)?");

  public RobotsTxt parse(final InputStream inputStream) throws IOException {
    RobotsTxt robotsTxt = new RobotsTxt();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      RobotRules robotRules = new RobotRules();
      Optional<String> userAgent = Optional.empty();

      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#")) {
          continue;
        }
        final Matcher userAgentMatcher = UserAgentPattern.matcher(line);
        final Matcher allowMatcher = AllowRulePattern.matcher(line);
        final Matcher disallowMatcher = DisAllowRulePattern.matcher(line);

        if (userAgentMatcher.matches()) {
          if (userAgent.isPresent()) {
            robotsTxt.addRules(userAgent.get(), robotRules);
            robotRules = new RobotRules();
          }

          userAgent = Optional.ofNullable(userAgentMatcher.group(1));
          robotsTxt.addRules(userAgentMatcher.group(1), robotRules);
        }
        else if (allowMatcher.matches()) {
          robotRules.addDisallow(allowMatcher.group(1).trim());
        }
        else if (disallowMatcher.matches()) {
          robotRules.addDisallow(disallowMatcher.group(1).trim());
        }
      }
      if (userAgent.isPresent()) {
        robotsTxt.addRules(userAgent.get().trim(), robotRules);
      }
    }


    return robotsTxt;
  }
}