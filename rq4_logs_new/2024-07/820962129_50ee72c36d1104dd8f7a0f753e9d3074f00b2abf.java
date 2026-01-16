package com.honestefforts.fixengine.model.validation;

import com.honestefforts.fixengine.model.universal.CountryCode;
import com.honestefforts.fixengine.model.universal.Currency;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum TagType {
  BOOLEAN(Pattern.compile("^[YN]$").asMatchPredicate()),
  STRING(Objects::nonNull),
  INTEGER(Pattern.compile("^-?[0-9]+$").asMatchPredicate()),
  POSITIVE_INTEGER(Pattern.compile("^[0-9]+$").asMatchPredicate()),
  DAY_OF_MONTH(Pattern.compile("^[0-3]?[0-9]$").asMatchPredicate()),
  COUNTRY(CountryCode::isAValidCountryCode),
  CURRENCY(Currency::isAValidCurrency),
  EXCHANGE(Pattern.compile("").asMatchPredicate()),
  LOCAL_DATE(Pattern.compile("").asMatchPredicate()),
  FLOAT(Pattern.compile("").asMatchPredicate());

  private final Predicate<String> typeValidationMatch;

  //private final Pattern regexMatch;

  public boolean isCompliant(String tagValue) {
    return typeValidationMatch.test(tagValue);
  }

  public static void main(String[] args) {
    System.out.println(COUNTRY.regexMatch.matcher("ABC").matches());
  }
}