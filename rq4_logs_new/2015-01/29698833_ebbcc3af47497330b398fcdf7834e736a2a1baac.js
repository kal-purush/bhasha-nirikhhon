var GoogleStrategy = require('passport-google-oauth').OAuth2Strategy;
var passport = require('passport');
var gcal     = require('google-calendar');
//refactor to promises later
var Promise = require('bluebird');
var Firebase = require('firebase')
var ref = new Firebase("https://fire-devil.firebaseio.com/")
//reminder to put into secret cofig file
var auth = require('./auth')
//neccesary passport functions
passport.serializeUser(function(user, done) {
  done(null, user);
});

passport.deserializeUser(function(user, done) {
  done(null, user);
});
//secret access information which allows protects users from CSFR 
passport.use(new GoogleStrategy({

        clientID        : auth['clientID'],
        clientSecret    : auth['clientSecret'],
        callbackURL     : auth['callbackURL'],

    },
    function(token, refreshToken, profile, done) {
        //instantiate new calendar of logged user
        google_calendar = new gcal.GoogleCalendar(token);
        var currentUser = profile.displayName
        // console.log('token: ',token)
        //makes a new firebase directory for the user
        
        //google api function to list calendars of user
        google_calendar.calendarList.list(function(err, calendarList){
          
          //referance to specific calendar
          var calendarId = calendarList.items[0].id;
          var calendarInfo = {};
          
          calendarInfo.user = currentUser;
          
            google_calendar.events.list(calendarId, function(err, calendarList) {
              //organizes the the data, callback to make sure the data is loaded
              //when the project  is scaled
              var getStableData = function(cb){
                for (var eve in calendarList.items){
                  if (calendarList.items[eve]['summary'] != undefined){
                    (function(eve){ 
                      calendarInfo[eve] = {}
                      var happening = calendarList.items[eve]['summary']
                      calendarInfo[eve]['event'] = happening;
                      calendarInfo.happening = happening;
                      calendarInfo[eve]['start'] =  calendarList.items[eve]['start']
                      calendarInfo[eve]['end'] = calendarList.items[eve]['end']
                      if (calendarList.items[eve]['recurrence']){
                        calendarInfo[eve]['recurrence'] = calendarList.items[eve]['recurrence']
                      }
                    })(eve)
                  

                  }         
                }
                cb();
              }
              //uploads data to firebase
              getStableData(function(){
                var usersRef = ref.child(currentUser);
                usersRef.set(calendarInfo);
              })
            });
          });
        
        process.nextTick(function() {
        return done(null, profile);
        });
    }
  ));
        