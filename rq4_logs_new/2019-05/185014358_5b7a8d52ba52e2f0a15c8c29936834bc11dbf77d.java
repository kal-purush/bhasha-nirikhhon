package com.coding.dojo.args;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class Args {
  private final Schemas schemas;
  private Map<Character, Argument> argsMap;

  public Args(String args, String schemasAstext) {
    this.schemas = new Schemas(schemasAstext);
    this.argsMap =
        Arrays.stream(args.substring(1).split("-"))
            .collect(
                Collectors.toMap(
                    s -> s.charAt(0),
                    s -> new Argument(s.charAt(0), StringUtils.trim(s.substring(1)))));
  }

  public Object getValue(Character flag) {
    return this.schemas.getArgsValue(flag, argsMap.get(flag).getValue());
  }
}