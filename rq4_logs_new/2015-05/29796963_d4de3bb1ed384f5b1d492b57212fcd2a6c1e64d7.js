if (typeof GmailNotes == "undefined") {
  var GmailNotes = { };
}

if (typeof(GmailNotes.BA) == "undefined") {
  GmailNotes.BA = {
    onload: function() {
      // we attempt to rerun GmailNotes when the Browser Action is clicked
      GmailNotes.BA.attemptRerun();
    },

    attemptRerun: function() {
      var request = { action: "rerun" };
      chrome.extension.sendRequest(request);
    }
  }; // end GmailNotes.BA
}

window.onload = GmailNotes.BA.onload;