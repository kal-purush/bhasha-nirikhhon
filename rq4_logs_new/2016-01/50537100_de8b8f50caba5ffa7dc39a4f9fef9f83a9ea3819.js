chrome.browserAction.onClicked.addListener(function(tab){
    chrome.bookmarks.getTree(function(resultsArray) {
        var bookmarks = [];
        addBookmarks(resultsArray, bookmarks, function() {
            chrome.tabs.create({
                "url": bookmarks[Math.floor(Math.random() * (bookmarks.length + 1))],
                "active": true
            }, function(tab){});
        });
    })
});

function addBookmarks(bookmarkTree, bookmarkArray, callback) {
    for (var i = 0; i < bookmarkTree.length; i++) {
        if (bookmarkTree[i].url) {
            bookmarkArray.push(bookmarkTree[i].url);
        } else if (bookmarkTree[i].children) {
            addBookmarks(bookmarkTree[i].children, bookmarkArray, function() {
            })
        }
    }
    return callback();
}