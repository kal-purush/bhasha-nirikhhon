chrome.contextMenus.create({
  title: "Twitterer",
  contexts: ["selection"],
  onclick: myFunction
});

function myFunction() {
  alert("You just clicked me choot.");
}