chrome.action.onClicked.addListener(() => {
  chrome.windows.create({
      url: "popup.html",
      type: "popup",
      width: 450,
      height: 600
  });
});