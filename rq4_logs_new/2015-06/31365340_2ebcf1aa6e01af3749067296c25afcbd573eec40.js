// Examples -- for text areas where multiline placeholders aren't allowed
var accountAgentExample = {
  "name": "learner",
  "homePage": "http://example.com"
};

var accountGroupExample = {
  "name": "Team Tin Can",
  "homePage": "http://tincanapi.com"
};

var groupExample = [
    {
        "name": "Bob",
        "account": {
            "homePage": "http://www.example.com",
            "name": "13936749"
        },
        "objectType": "Agent"
    },
    {
        "name": "Alice",
        "mbox_sha1sum": "ebd31e95054c018b10727de4db3ef2ec3a016ee9",
        "objectType": "Agent"
    }
];

var groupExample2 = [
    {
        "name": "Bob",
        "account": {
            "homePage": "http://www.example.com",
            "name": "13936749"
        },
        "objectType": "Agent"
    },
    /*{
        "name": "Carol",
        "openid": "http://carol.openid.example.org/",
        "objectType": "Agent"
    },*/
    {
        "name": "Tom",
        "mbox": "mailto:tom@example.com",
        "objectType": "Agent"
    },
    {
        "name": "Alice",
        "mbox_sha1sum": "ebd31e95054c018b10727de4db3ef2ec3a016ee9",
        "objectType": "Agent"
    }
];

var substatementExample = {
    "actor" : {
        "objectType": "Agent",
        "mbox":"mailto:test@example.com" 
    },
    "verb" : { 
        "id":"http://example.com/visited", 
        "display":{
            "en-US":"will visit"
        } 
    },
    "object": {
        "objectType": "Activity",
        "id":"http://example.com/website",
        "definition": { 
            "name" : {
                "en-US":"Some Awesome Website"
            }
        }
    }
};

var contextActivitiesExample = {
  "parent": [
    {
      "definition": {
        "name": {
          "en-US": "Tin Can Lab"
        },
        "description": {
          "en-US": "Assisting in developing statements and communicating with a Learning Record Store (LRS)"
        }
      },
      "id": "http://tincanapi.com/tin-can-lab",
      "objectType": "Activity"
    },
    {
      "definition": {
        "name": {
          "en-US": "Statement Builder Context"
        }
      },
      "id": "http://tincanapi.com/tin-can-lab/tincan.html#context",
      "objectType": "Activity"
    }
  ]
};