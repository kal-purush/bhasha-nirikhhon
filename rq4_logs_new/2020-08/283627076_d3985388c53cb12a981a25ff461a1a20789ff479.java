package com.treasureisland.items;

import java.util.ArrayList;
import java.util.Arrays;

public class ItemBuilder {
  private final ArrayList<String> itemsAndScenes =
      new ArrayList<>(
          Arrays.asList(
              "watch:forest",
              "flashlight:forest",
              "lockbox:house",
              "lighter:house",
              "toilet_paper:abandonedcar",
              "torch:forest",
              "boat:cave",
              "blade:field",
              "key:shed"));

  /*
   * =============================================
   * ============= Constructors ==================
   * =============================================
   */

  public ItemBuilder() {}

  /*
   * =============================================
   * =========== Business Methods ================
   * =============================================
   */

  /** @return allItems -> an ArrayList of all Items in game */
  public ArrayList<Item> buildAllItems() {
    String itemName;
    String currentScene;
    boolean isAnotherItemNeeded = false;
    String nameOfOtherItem = "none";

    Item anItem;
    ArrayList<Item> allItems = new ArrayList<>();

    for (String itemAndScene : itemsAndScenes) {
      String[] itemAndSceneNames = itemAndScene.split(":");

      itemName = itemAndSceneNames[0];
      currentScene = itemAndSceneNames[1];

      if (itemName.equals("lockbox")) {
        isAnotherItemNeeded = true;
        nameOfOtherItem = "key";
      }

      if (itemName.equals("torch")) {
        isAnotherItemNeeded = true;
        nameOfOtherItem = "lighter";
      }

      anItem = new Item();

      allItems.add(anItem);
    }

    return allItems;
  }
}