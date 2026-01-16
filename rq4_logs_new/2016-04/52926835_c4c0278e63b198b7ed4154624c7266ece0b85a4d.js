const list = new Mongo.Collection("players");
const tourny = new Mongo.Collection("tourny");
const tournyList = new Mongo.Collection("tourn");
const game = new Mongo.Collection("game");
const attempt = new Mongo.Collection("attempt");
//only for navigation bar makes it pretty based on what page you are on
var changeColor =function(num){
  isError(false);
  var x = document.getElementsByClassName("navigate");
  for(var i=0; i<x.length; i++){
    x[i].style.backgroundColor="#004b66";
  }
  x[num].style.backgroundColor="#ffa31a";
};

//returns if the passed value is an integer
var isInt= function(value) {
  if ((undefined === value) || (null === value) || (value == "")) {
    return false;
  }
  return value % 1 == 0;
};

//if value is true will display an error to the screen, otherwise wipes it out
var isError=function(value){
  document.getElementById("error").innerHTML = "";
  if(value)
    document.getElementById("error").innerHTML = "<b>*ERROR</b>";
  else
    document.getElementById("error").innerHTML = "";
};


  //used to set dynamic template
  Session.set("templateName", "home");
  Session.set("orderBy", "playerID");
  Session.set("tournyID", 1);
  Session.set("playerID", -1);

  //will fill the table with values from the database
  Template.playerPage.helpers({
    players(){
      var order = Session.get("orderBy");
      if(order == "playerID")
        return list.find({}, {sort:{playerID:1}});
      else if(order == "first")
        return list.find({}, {sort:{firstName:1}});
      else if(order == "last")
        return list.find({}, {sort:{lastName:1}});
      else if(order == "age")
        return list.find({}, {sort:{age:1}});
      else if(order == "gender")
        return list.find({}, {sort:{gender:1}});
    },
  });

  Template.gameHelper.helpers({
    ids(){
      return list.find({},{sort:{playerID:1}});
    }
  });
  Template.gameTable.helpers({
    frames(){
      return [{"frame":1},{"frame":2},{"frame":3},{"frame":4},{"frame":5},{"frame":6},{"frame":7},{"frame":8},{"frame":9},{"frame":10}];
    },
    attempts(){
      return [{"attempt":1},{"attempt":2},{"attempt":3}];
    },
    games(){
      return game.find();
    }
  });

  Template.tournyHelper.helpers({
    name:function(){
      var tournyNum = Session.get("tournyID");
      return tournyList.findOne({tournyID:tournyNum}).tournyName;
    },
    date:function(){
      var tournyNum = Session.get("tournyID");
      return tournyList.findOne({tournyID:tournyNum}).date;
    },
    location:function(){
      var tournyNum = Session.get("tournyID");
      return tournyList.findOne({tournyID:tournyNum}).location;
    },
    sponsor:function(){
      var tournyNum = Session.get("tournyID");
      return tournyList.findOne({tournyID:tournyNum}).sponsor;
    },
    teamPlaces(){
      var tournyNum = Session.get("tournyID");
      return tourny.find({tournyID:tournyNum},{sort:{teamPlace:1}});
    },
    singlePlaces(){
      var tournyNum = Session.get("tournyID");
      return tourny.find({tournyID:tournyNum},{sort:{singlePlace:1}});
    },
    ids(){
      return tournyList.find({},{sort:{tournyID:1}});
    }
  });

  //changes which page is being displayed
  Template.dashboard.helpers({
    templateName: function(){
      return Session.get("templateName");
    },
  });

  Template.tournyHelper.events({
    'submit form': function(event){
      event.preventDefault();
      var num=Number(event.target.Tournament.value);
      Session.set("tournyID", num);
    }
  });

  //listens to the navigation bar and sets the page being seen
  Template.dashboard.events({
      'click .logout': function(event){
          event.preventDefault();
          Meteor.logout();
      },
    "click .home": function(){
      changeColor(0);
      Session.set("templateName", "home");
    },
    "click .playerPage": function(){
      changeColor(1);
      Session.set("templateName", "playerPage");
    },
    "click .tournamentPage": function(){
      changeColor(2);
      Session.set("templateName", "tournamentPage");
    },
    "click .gamePage": function(){
      changeColor(3);
      Session.set("templateName", "gamePage");
    }
  });


  //will remove a player when the playerID is hit
  Template.player.events({
    'click button': function(event)
    {
      event.preventDefault();
      var pID = Number(event.target.innerHTML);
      var deleteThis = list.findOne({"playerID":pID});
      list.remove({"_id":deleteThis._id});
    }
  });

  //orders the player page
  Template.order.events({
    'submit form': function(event)
    {
      event.preventDefault();
      var orderBy=event.target.orderBy.value;
      Session.set("orderBy", orderBy);
    }
  });

  //template for adding a new player to the list, checks to see if the values
  //meet certain criteria
  Template.add.events({
    'submit form': function(event)
    {
      event.preventDefault();

      // get values from forms
      var playerID = Math.floor((Math.random() * 900000) + 100000);
      while(list.findOne({"playerID":playerID})!=null)
        playerID = Math.floor((Math.random() * 900000) + 100000);
      var firstName = event.target.first.value;
      var lastName = event.target.last.value;
      var age = event.target.age.value;
      var gender = event.target.gender.value;
      gender = gender.toUpperCase();
      if(firstName=="" || lastName=="" || !isInt(age) || gender == "")
      {
        isError(true);
      }
      else
      {
        isError(false);
        // insert into the database
        list.insert({playerID:playerID, firstName:firstName, lastName:lastName, age:age, gender:gender});
        // empty all the forms
        event.target.first.value="";
        event.target.last.value="";
        event.target.age.value="";
        event.target.gender.selectedIndex=0;
      }

    }
  });

  //listens to the update form and checks the criteria before updating the given value
  Template.update.events({
    'submit form': function(event)
    {
      event.preventDefault();
      // get values from forms
      var playerID = Number(event.target.playerID.value);
      var firstName = event.target.first.value;
      var lastName = event.target.last.value;
      var age = event.target.age.value;
      var gender = event.target.gender.value;
      gender = gender.toUpperCase();

      var updateThis = list.findOne({"playerID":playerID});
      if(updateThis!=undefined)
      {
        isError(false);
        if(firstName!="")
          list.update( {"_id": updateThis._id}, {$set: { "firstName": firstName}});
        if(lastName!="")
          list.update({"_id": updateThis._id}, {$set: { "lastName": lastName}});
        if(isInt(age))
          list.update( {"_id": updateThis._id}, {$set: { "age": age}});
        if(gender!="")
          list.update( {"_id": updateThis._id}, {$set: { "gender": gender}});
        // empty all the forms
        event.target.playerID.value="";
        event.target.first.value="";
        event.target.last.value="";
        event.target.age.value="";
        event.target.gender.selectedIndex=0;
      }
      else
        isError(true);

    }
  });

  Template.addResult.events({
    'submit form': function(event)
    {
      event.preventDefault();
      // get values from forms
      var teamName = event.target.teamName.value;
      var teamPlace = Number(event.target.teamPlace.value);
      var teamScore = Number(event.target.teamScore.value);
      var singleScore = Number(event.target.singleScore.value);
      var singlePlace = Number(event.target.singlePlace.value);
      if(teamName=="" || !isInt(teamPlace) || !isInt(teamScore) || !isInt(singleScore) || !isInt(singlePlace))
      {
        isError(true);
      }
      else
      {
        isError(false);
        // insert into the database
        var num = Session.get("tournyID");
        tourny.insert({tournyID:num, teamName:teamName, teamPlace:teamPlace, teamScore:teamScore, singleScore:singleScore, singlePlace:singlePlace});
        // empty all the forms
        event.target.teamName.value="";
        event.target.teamPlace.value="";
        event.target.teamScore.value="";
        event.target.singleScore.value="";
        event.target.singlePlace.value="";
      }
    }
  });

  Template.updateResult.events({
    'submit form': function(event)
    {
      event.preventDefault();
      // get values from forms
      var teamName = event.target.teamName.value;
      var teamPlace = Number(event.target.teamPlace.value);
      var teamScore = Number(event.target.teamScore.value);
      var singleScore = Number(event.target.singleScore.value);
      var singlePlace = Number(event.target.singlePlace.value);

      var updateThis = tourny.findOne({"teamName":teamName});
      if(updateThis!=undefined)
      {
        isError(false);
        if(isInt(teamPlace))
          tourny.update( {"_id": updateThis._id}, {$set: { "teamPlace":teamPlace}});
        if(isInt(teamScore))
          tourny.update({"_id": updateThis._id}, {$set: { "teamScore":teamScore}});
        if(isInt(singlePlace))
          tourny.update( {"_id": updateThis._id}, {$set: { "singlePlace":singlePlace}});
        if(isInt(singleScore))
          tourny.update( {"_id": updateThis._id}, {$set: { "singleScore":singleScore}});
        // empty all the forms
        event.target.teamName.value="";
        event.target.teamPlace.value="";
        event.target.teamScore.value="";
        event.target.singleScore.value="";
        event.target.singlePlace.value="";
      }
      else
        isError(true);
    }
  });
  Template.newTournament.events({
    'submit form': function(event)
    {
      event.preventDefault();
      // get values from forms
      var num = Math.floor(Math.random() * 100 + 1);
      var tournyName = event.target.tournyName.value;
      var location = event.target.location.value;
      var date = event.target.date.value;
      var sponsor = event.target.sponsor.value;
      if(tournyName == "" || location=="" || date=="" || sponsor=="")
      {
        isError(true);
      }
      else
      {
        isError(false);
        // insert into the database
        tournyList.insert({tournyID:num, tournyName:tournyName, location:location, date:date, sponsor:sponsor});
        // empty all the forms
        event.target.tournyName.value="";
        event.target.date.value="";
        event.target.location.value="";
        event.target.sponsor.value="";
      }
    }
  });

  Template.gameTable.events({
    'submit form': function(event){
      event.preventDefault();
      var x=[0,0,0,0,0,0,0,0,0,0];

      x[0] = event.target.p1.checked ? 1:0;
      x[1] = event.target.p2.checked ? 1:0;
      x[2] = event.target.p3.checked ? 1:0;
      x[3] = event.target.p4.checked ? 1:0;
      x[4] = event.target.p5.checked ? 1:0;
      x[5] = event.target.p6.checked ? 1:0;
      x[6] = event.target.p7.checked ? 1:0;
      x[7] = event.target.p8.checked ? 1:0;
      x[8] = event.target.p9.checked ? 1:0;
      x[9] = event.target.p10.checked ? 1:0;

      attempt.insert({"attempt":event.target.attempt.value, "frame":event.target.frame.value, "gameID":event.target.gameID.value, "p1":x[0], "p2":x[1], "p3":x[2], "p4":x[3], "p5":x[4], "p6":x[5], "p7":x[6], "p8":x[7], "p9":x[8], "p10":x[9]});

      event.target.attempt.select = 0;
      event.target.frame.select = 0;


      event.target.p1.checked = false;
      event.target.p2.checked = false;
      event.target.p3.checked = false;
      event.target.p4.checked = false;
      event.target.p5.checked = false;
      event.target.p6.checked = false;
      event.target.p7.checked = false;
      event.target.p8.checked = false;
      event.target.p9.checked = false;
      event.target.p10.checked = false;
    }
  });

  Template.gameHelper.events({
    'submit form':function(event){
      event.preventDefault();
      Session.set("playerID", event.target.players.value);
    }
  });


if (Meteor.isServer) {
  Meteor.startup(function () {
    // code to run on server at startup
  });
}