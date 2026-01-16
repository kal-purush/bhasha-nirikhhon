

// FIREBASE
  // Initialize Firebase
  var config = {
    apiKey: "AIzaSyDfvSmJmO2KbcRm_H3d9QaxbVkUafa0R0Y",
    authDomain: "tailfeather2018um.firebaseapp.com",
    databaseURL: "https://tailfeather2018um.firebaseio.com",
    projectId: "tailfeather2018um",
    storageBucket: "tailfeather2018um.appspot.com",
    messagingSenderId: "246990159474"
  };


  firebase.initializeApp(config);



  const auth = firebase.auth();
  const database = firebase.database();


  

  
//Shared secret 539fbc65bef57dca35929b3ef2c22e10
//Registered to mricart881



  var app = {

    userName: null,
    currentResult: null,

    ///////////////////////////////////////////////
    ////////////     User MGMT     ////////////////
    ///////////////////////////////////////////////
    createUser: function(email, pass, name, lname){
      auth.createUserWithEmailAndPassword(email,pass)
      

      .catch(function(error) {
            // Handle Errors here.
            var errorCode = error.code;
            var errorMessage = error.message;
            // ...
            console.log(errorMessage)

      });

      setTimeout(function(){
        app.updateProfileInfo(name,lname);
        window.location.reload();

      }, 500);
    },

    signIn: function(email, pass){
      auth.signInWithEmailAndPassword(email,pass)

      .catch(function(error) {

              var showEmailError = function(){
                if($("#sign-in-modal-wrong-user-name").length === 0){
                  $errorP = $("<p id='sign-in-modal-wrong-user-name'>").text("That user does not exist. Sign up now!").css("color", "red");
                }
              }

              // Handle Errors here.
              var errorCode = error.code;
              var errorMessage = error.message;
              var $errorP;
              // ...
              if(errorCode === "auth/invalid-email"){
                $("#sign-in-email").css("border", "1px solid red");

                showEmailError();
                return $(".modal-footer").prepend($errorP);
              }
              else if(errorCode = "auth/wrong-password"){
                return $("#sign-in-password").css("border", "1px solid red");
              }
              
        });
    },

    signOut: function(){
      auth.signOut();
      $("#signedInName").remove();

    },

    //keep track of userSignedIn
    userSignedIn: false,
    isUserSignedIn: function(){


      firebase.auth().onAuthStateChanged(function(user) {
        if (user) {
          
          $("#sign-in-modal").modal('hide');
          $("#sign-out-button").removeClass('hide');
          app.getUserName();
          return app.userSignedIn = true;

        } else {
          $("#sign-out-button").addClass('hide');
          return app.userSignedIn = false;
        }
      });
    },

    updateProfileInfo: function(name, lname){
      
          database.ref().child(auth.currentUser.uid).set({"name": name, "lname": lname});
        
          
    },

    //we use this function to validate password and callback a function
    // if the password is correct for the current user
    validatePassThenCall: function(pass, MyFunction){
      $.when(firebase.auth().currentUser.reauthenticateWithCredential(firebase.auth.EmailAuthProvider.credential(firebase.auth().currentUser.email, pass)))
          .done(function(){
            MyFunction;
          })
          .catch(function(error){ 

            console.log(error.message);
            

          });

    },



    ///////////////////////////////////////////////
    //////////      to/from UI      /////////////
    ///////////////////////////////////////////////

    //#sign-in-form
    //#sign-in-email
    //#sign-in-password

    resetSignInForm: function(){

      $("#sign-in-modal-wrong-user-name").remove();
      
      $("#sign-in-email").css("border", "1px solid #ced4da");
      $("#sign-in-password").css("border", "1px solid #ced4da");
      $("#sign-in-email").val("");
      $("#sign-in-password").val("");

    },

    getUserName: function(){
        
        
          database.ref(auth.currentUser.uid + "/").on("value", function(snapshot){

            if($("#signedInName").length < 1){
              var $displayName = $("<p id='signedInName'>").text(`Signed In as: ${snapshot.val().name} ${snapshot.val().lname}`);
              $("#header").append($displayName);
            }
          });
        
    }


  }




////////\
///////__\
//////
/////
////
///
//on document ready




  $(function(){


//////////////////////////////////////////////////
////////////       Click Events      /////////////
//////////////////////////////////////////////////

    //start listening for Authentication changes
    app.isUserSignedIn();
    //listen for username
    
    
    

    //click event for profile icon top right
    //if user not logged in, modal log in
    //else... send to profile
    $("#header").on("click", "#profileIcon", function(){
      if(app.userSignedIn === false){
        app.resetSignInForm();
        $("#sign-in-modal").modal('toggle');


      }
      else{
        console.log("Send to profile");
      }
    });

    //submit event on login form
    $("#sign-in-form").on("submit", function(e){
      
      e.preventDefault();

      


      //get Email and PW from form
      var email = $("#sign-in-email").val();
      var pass = $("#sign-in-password").val();

      //attempt sign-in
      app.signIn(email,pass);

      //reset form incase we caught errors before closing the modal
      app.resetSignInForm();

      
    });


    //click event for signOut button in header
    $("#header").on("click", "#sign-out-button", function(e){

      app.signOut();
    });


    //click event signup button in sign-in-modal
    $("#sign-in-modal").on("click", "#sign-up-button", function(){

        $("#sign-in-form").remove();
        var $form = $("<form id='sign-up-form'>");
        var $body = $("<div class='modal-body'>").appendTo($form);
        


        var $name = $("<div>").addClass("form-group");
        $("<label>").addClass("for", "sign-up-name").text("Name").appendTo($name);
        $("<input>").attr({"type": "input", "id": "sign-up-name", "class": "form-control", "placeholder": "Enter name"}).appendTo($name);

        var $lname = $("<div>").addClass("form-group");
        $("<label>").addClass("for", "sign-up-lname").text("Last Name").appendTo($lname);
        $("<input>").attr({"type": "input", "id": "sign-up-lname", "class": "form-control", "placeholder": "Enter last name"}).appendTo($lname);

        var $email = $("<div>").addClass("form-group");
        $("<label>").addClass("for", "sign-up-email").text("Email").appendTo($email);
        $("<input>").attr({"aria-describedby": "emailHelp", "type": "email", "id": "sign-up-email", "class": "form-control", "placeholder": "Enter email"}).appendTo($email);

        var $pass = $("<div>").addClass("form-group");
        $("<label>").addClass("for", "sign-up-password").text("Password").appendTo($pass);
        $("<input>").attr({"type": "password", "id": "sign-up-password", "class": "form-control", "placeholder": "Password"}).appendTo($pass);


        $body.prepend($name, $lname, $email, $pass);
        $form.appendTo($(".modal-body"));
        
        $("<input>").attr({"id": "sign-up-form-button", "class": "btn btn-success", "type": "submit", "value": "Sign Up!"}).appendTo($body);
        $("#sign-up-button").remove();
        $("#sign-in-button").remove();

        
        
    });



    //submit event on sign-up form
    $("#sign-in-modal").on("submit", "#sign-up-form", function(e){
      e.preventDefault();

      var $name = $("#sign-up-name").val();
      var $lname = $("#sign-up-lname").val();
      var $email = $("#sign-up-email").val();
      var $pass = $("#sign-up-password").val();

      console.log($email + $pass + $name + $lname);
      app.createUser($email, $pass, $name, $lname);

      $("sign-in-modal").modal("hide");

    });


    // Hide modal event reload to reset sign in
    $('#sign-in-modal').on('hidden.bs.modal', function () {
    // do something…
      window.location.reload();
    })
    
      
    
  });


  


  
