var re = /http:\/\/www.reddit.com\/r\/(\w+)\/comments\/(\w+)/;
var cachedComments = new Set();

/*
 * Functions to sort comments.
 */
var sortBest = function(a, b) {
    if (a.score === undefined) return 1;
    if (b.score === undefined) return -1;
    
    if (a.score > b.score) return -1;
    if (b.score > a.score) return 1;
    return 0;
}

var sortRecent = function(a, b) {
    if (a.created_utc === undefined) return 1;
    if (b.created_utc === undefined) return -1;
    
    if (a.created_utc > b.created_utc) return -1;
    if (b.created_utc > a.created_utc) return 1;
    return 0;
}

/*
 * Format the display of the comment.
 */
var formatCommentHTML = function(commentData) {
    var seconds = Math.round(Date.now() / 1000);
    var secAgo =  seconds - commentData.created_utc;
    var htmlText = "";
    htmlText += secAgo + " sec ago... <br />";
    htmlText += "<b>" + commentData.author + "</b>";
    htmlText += " (" + commentData.score + ") " + " Flair: " + commentData.author_flair_text + "<br />";
    htmlText += commentData.body;
    return htmlText;
}

/*
 * Submit AJAX request to get latest 200 comments and display the top 5
 * by score.
 */
var updateComments = function(subreddit, thread) {
    reddit.comments(thread, subreddit).limit(200).sort("new").fetch(function(res) {
        var displayableComments = [];
        var comments = res[1].data.children;
        
        var c;
        for (var i = 0; i < comments.length; i++) {
            c = comments[i].data;
            if (!cachedComments.has(c.id)) {                
                displayableComments.push(c);
            }
        }
        
        displayableComments.sort(sortBest);        
        var top5 = displayableComments.slice(0, 5);
        top5.sort(sortRecent);
        
        var commentBox;
        for (i = 0; i < 5; i++) {            
            commentBox = document.getElementById("comment" + i);
            commentBox.innerHTML = formatCommentHTML(top5[i]);
            cachedComments.add(top5[i].id);
        }
        
        //console.log(top5);
    });
}

/*
 * Functions that get called by buttons.
 */
function update() {
    var subreddit = document.getElementById("subreddit");
    var thread = document.getElementById("thread");
    if (subreddit.value != null && subreddit.value != "" &&
        thread.value != null && thread.value !== "") {
        updateComments(subreddit.value, thread.value);
    }
}

function parse() {
    var url = document.getElementById("redditURL");
    if (url.value == null || url.value == "") return;
    var match = re.exec(url.value);
    if (match.length < 3) return;
    document.getElementById("subreddit").value = match[1];
    document.getElementById("thread").value = match[2];    
}

/**
 * Attach listeners after elements have been loaded.
 */
window.addEventListener("load",function(){
    document.getElementById("parseButton").addEventListener("click", parse, false);
    document.getElementById("updateButton").addEventListener("click", update, false);
});