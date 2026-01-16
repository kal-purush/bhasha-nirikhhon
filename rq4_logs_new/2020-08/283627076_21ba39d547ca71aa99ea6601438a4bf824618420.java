package com.treasureisland;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum Interactions {
  T("T", "TALK"),
  L("L", "LOOK"),
  R("R", "REWARD"),
  M("M", "MAP"),
  INV("INV", "INVENTORY"),
  G("G", "GRAB"),
  V("V", "VENDOR"),
  E("E", "EXIT"),
  S("S", "SAVE"),
  C("C", "CHART");




  private String acceptedText;
  private String otherAcceptedText;

  Interactions(String acceptedText, String otherAcceptedText) {
    this.acceptedText = acceptedText;
    this.otherAcceptedText = otherAcceptedText;
  }

  public List<String> getAcceptedText() {
    return List.of(acceptedText, otherAcceptedText);
  }

  public static Boolean isValid(String userInput) {
    boolean isValid = false;
    if (fromString(userInput) != null) {
      isValid = true;
    }
    return isValid;
  }

  public static Interactions fromString(String acceptedText) {
    Interactions foundInteraction = null;

    for (Interactions interaction : Interactions.values()) {
      if (interaction.acceptedText.equalsIgnoreCase(acceptedText) || interaction.otherAcceptedText.equalsIgnoreCase(acceptedText)) {
        foundInteraction = interaction;
        break;
      }
    }
    return foundInteraction;
  }

}