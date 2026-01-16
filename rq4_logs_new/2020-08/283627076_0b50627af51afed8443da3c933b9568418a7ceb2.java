package com.treasureisland.items;

import java.io.Serializable;

public class Item implements Serializable {
  private String itemName;
  private String currentWorld;

  private Integer healthValue;
  private Integer cost;

  private Boolean isAnotherItemNeeded = false;
  private String purpose = "none";
  private String nameOfOtherItem = "none";
  private String whatDoesOtherItemDo = "n/a";
  private String itemRevealed = "none";
  private String asciiArt = "none";

  /*
   * =============================================
   * ============= Constructors ==================
   * =============================================
   */
  public Item() {}

  public Item(String itemName) {
    setItemName(itemName);
  }

  public Item(String itemName, Integer healthValue, Integer cost) {
    this(itemName);
    setHealthValue(healthValue);
    setCost(cost);
  }

  /*
   * =============================================
   * =========== Business Methods ================
   * =============================================
   */

  /*
   * =============================================
   * =========== Accessor Methods ================
   * =============================================
   */

  // SET METHODS
  public void setItemName(String itemName) {
    this.itemName = itemName;
  }

  public void setCurrentWorld(String currentWorld) {
    this.currentWorld = currentWorld;
  }

  public void setHealthValue(Integer healthValue) {
    this.healthValue = healthValue;
  }

  public void setCost(Integer cost) {
    this.cost = cost;
  }

  public void setPurpose(String purpose) {
    this.purpose = purpose;
  }

  public void setAnotherItemNeeded(Boolean anotherItemNeeded) {
    isAnotherItemNeeded = anotherItemNeeded;
  }

  public void setNameOfOtherItem(String nameOfOtherItem) {
    this.nameOfOtherItem = nameOfOtherItem;
  }

  public void setWhatDoesOtherItemDo(String whatDoesOtherItemDo) {
    this.whatDoesOtherItemDo = whatDoesOtherItemDo;
  }

  public void setItemRevealed(String itemRevealed) {
    this.itemRevealed = itemRevealed;
  }

  public void setAsciiArt(String asciiArt) {
    this.asciiArt = asciiArt;
  }

  // GET METHODS

  public String getItemName() {
    return itemName;
  }

  public String getCurrentWorld() {
    return currentWorld;
  }

  public Integer getHealthValue() {
    return healthValue;
  }

  public Integer getCost() {
    return cost;
  }

  public String getPurpose() {
    return purpose;
  }

  public Boolean getAnotherItemNeeded() {
    return isAnotherItemNeeded;
  }

  public String getNameOfOtherItem() {
    return nameOfOtherItem;
  }

  public String getWhatDoesOtherItemDo() {
    return whatDoesOtherItemDo;
  }

  public String getItemRevealed() {
    return itemRevealed;
  }

  public String getAsciiArt() {
    return asciiArt;
  }


  @Override
  public String toString() {
    return "Items{" +
      "itemName='" + itemName + '\'' +
      ", currentWorld='" + currentWorld + '\'' +
      ", healthValue=" + healthValue +
      ", cost=" + cost +
      ", isAnotherItemNeeded=" + isAnotherItemNeeded +
      ", purpose='" + purpose + '\'' +
      ", nameOfOtherItem='" + nameOfOtherItem + '\'' +
      ", whatDoesOtherItemDo='" + whatDoesOtherItemDo + '\'' +
      ", itemRevealed='" + itemRevealed + '\'' +
      ", asciiArt='" + asciiArt + '\'' +
      ", anotherItemNeeded=" + getAnotherItemNeeded() +
      '}';
  }
}