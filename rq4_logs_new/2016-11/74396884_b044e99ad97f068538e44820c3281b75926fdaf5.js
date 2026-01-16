var firebase = require('firebase-admin');
var request = require('request');

var API_KEY = "AIzaSyCT1fxZmePaWESKmafVK4FBvdpV7BGO9Fs"; // Your Firebase Cloud Messaging Server API key

// Fetch the service account key JSON file contents
var serviceAccount = require("./serviceAccountKey.json");

// Initialize the app with a service account, granting admin privileges
firebase.initializeApp({
  credential: firebase.credential.cert(serviceAccount),
  databaseURL: "https://friendlychat-28062.firebaseio.com"
});
ref = firebase.database().ref();

function listenForNotificationRequests() {
  console.log("Listening for notificationRequests");
  var requests = ref.child('notificationRequests');
  requests.on('child_added', function(requestSnapshot) {
    var newChild = requestSnapshot.val(),
        groupID = newChild.groupID,
        senderID = newChild.senderID,
        payload = newChild.payload,
        title = newChild.title;

    console.log("Got the child " + JSON.stringify(newChild));
    getUsersForGid(newChild, groupID);
    requestSnapshot.ref.remove();
  }, function(error) {
    console.error(error);
  });
};

function getUsersForGid(messageChild, groupID) {
  var groupNode = ref.child('groups').child(groupID);
  groupNode.once('value', function(dataSnapshot){
      var usersImgs = dataSnapshot.val().usersImgs;
      var uids = Object.keys(usersImgs);
      console.log("Got usersIds " + uids);
      sendNotificationForUids(messageChild, uids);
  });
}; 

function sendNotificationForUids(messageChild, uids) {
    var tokens, pushToken, userTokenNode, uid,
        tokenNode = ref.child('pushTokens');
    for (i in uids) {
        uid = uids[i];
        console.log("Fetching PushTokens for uid " + uid);
        tokenNode.child(uid).once('value', function(dataSnapshot) {
            userTokenNode = dataSnapshot.val();
            console.log("Got Token Node" + userTokenNode);
            if (!userTokenNode) {return;}
            tokens = Object.keys(dataSnapshot.val());
            for (i in tokens) {
                pushToken = tokens[i];
                sendNotificationToUser(messageChild, pushToken);
            }
        });
    }
};

// example curl 
// curl --header "Authorization: key=AIzaSyCT1fxZmePaWESKmafVK4FBvdpV7BGO9Fs" --header Content-Type:"application/json" "https://fcm.googleapis.com/fcm/send" -d "{\"notification\":{\"title\": \"hello from terminal\"}, \"to\":\"fU-ml43e71g:APA91bH2s_rM_MdFUajjB2XyyvmDJ5mINkUSJ47VOz5lTQ_C5VSvsEpsHOeq_VkfiKRwDiSOXvbTfO6cRu-z4n_mH4b8pfLlOq9Ho1dC7UpIMCIx93IdHDQjyagAS54h-eSsRC7vwoUA\"}"
function sendNotificationToUser(child, fcmToken) {
    var groupID = child.groupID,
        senderID = child.senderID,
        payload = child.payload,
        title = child.title;

  request({
    url: 'https://fcm.googleapis.com/fcm/send',
    method: 'POST',
    headers: {
      'Content-Type' :' application/json',
      'Authorization': 'key='+API_KEY
    },
    body: JSON.stringify({
      notification: {
        title: title,
        body: payload,
      },
      data: child,
      to : fcmToken
    })
  }, function(error, response, body) {
    if (error) { console.error(error); }
    else if (response.statusCode >= 400) { 
      console.error('HTTP Error: '+response.statusCode+' - '+response.statusMessage); 
    }
    else {
      console.log("Sent the message " + title + " to " + fcmToken);
    }
  });
}

// start listening
listenForNotificationRequests();