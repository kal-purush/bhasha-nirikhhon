"use strict";

let equalizer = function () {
  let equalizedGroups = document.querySelectorAll(".equalized-group");
  equalizedGroups.forEach((equalizedGroup) => {
    let equalizedItems = equalizedGroup.querySelectorAll(".equalized-item");
    let highest = 0;
    equalizedItems.forEach((equalizedItem) => {
      if (equalizedItem.offsetHeight > highest)
        highest = equalizedItem.offsetHeight;
    });
    equalizedItems.forEach((equalizedItem) => {
      equalizedItem.style.height = highest + "px";
    });
    let equalizedItemsAlt = equalizedGroup.querySelectorAll(
      ".equalized-item-alt"
    );
    let highestAlt = 0;
    equalizedItemsAlt.forEach((equalizedItemAlt) => {
      if (equalizedItemAlt.offsetHeight > highestAlt)
        highestAlt = equalizedItemAlt.offsetHeight;
    });
    equalizedItemsAlt.forEach((equalizedItemAlt) => {
      equalizedItemAlt.style.height = highestAlt + "px";
    });
  });
};
window.addEventListener("load", equalizer);