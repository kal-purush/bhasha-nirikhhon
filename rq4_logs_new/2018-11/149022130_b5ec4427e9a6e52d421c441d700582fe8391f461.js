$(document).ready(function() {
  window.final = {};
  final.modalOptions = {
    'keyboard': false,
    'backdrop': 'static'
  };

  // option groups
  final.groups = [
    ["Butterfly", "Mosquito", "Moth", "Cicada"],
    ["Grapes", "Cranberry", "Lemon", "Apple"],
    ["English", "French", "Chinese", "Arabic"],
    ["Bass", "Trout", "Salmon", "Tuna"],
    ["Sofa", "Table", "Bed", "Chair"],
    ["Ammonia", "Chlorine", "Acetone", "Ethanol"],
    ["Car", "Boat", "Bus", "Train"],
    ["Celery", "Broccoli", "Cabbage", "Cauliflower"],
    ["Merlot", "Champagne", "Cabernet", "Chardonnay"],
    ["Oak", "Maple", "Walnut", "Mahogany"],
    ["Padauk", "Bubinga", "Cocobolo", "Poculi"],
    ["Sunflower", "Canola", "Sesame", "Olive"],
    ["Cup", "Goblet", "Glass", "Mug"],
    ["Sapphire", "Ruby", "Diamond", "Pearl"],
    ["Toothpaste", "Floss", "Mouthwash", "Toothbrush"],
    ["Python", "Java", "Ruby", "Go"],
    ["Rock", "Jazz", "Pop", "Country"],
    ["Windows", "iOS", "Linux", "MacOS"],
    ["Ruler", "Compass", "Protractor", "Angle"],
    ["Adidas", "Nike", "Reebok", "Puma"],
    ["Beyonce", "Prince", "Beatles", "Katy Perry"],
    ["Airplane", "Helicopter", "Blimp", "Rocket"],
    ["Daffodil", "Rose", "Tulip", "Pansy"],
    ["Rucksack", "Bag", "Backpack", "Satchel"]];
  final.groups = _.shuffle(final.groups);
  for (var i = 0; i < final.groups.length; i++) {
    final.groups[i] = _.shuffle(final.groups[i]);
  }

  final.practiceGroups = [
    ["Starfruit", "Durian", "Pomegranate", "Passionfruit"],
    ["Alphonso", "Chaunsa", "Fazli", "Raspuri"],
    ["Flounder", "Bream", "Haddock", "Mackerel"],
    ["Sicilian", "Chicago", "New York", "Thin Crust"],
    ["Tree", "Flower", "Bamboo", "Seaweed"],
    ["MongoDB", "Postgres", "MonetDB", "Firebase"],
    ["Fir", "Alder", "Cedar", "Poplar"],
    ["Tomato", "Eggplant", "Cucumber", "Squash"],
    ["Drawer", "Shelves", "Wardrobe", "Desk"],
    ["Mewtwo", "Ghastly", "Meowth", "Pikachu"],
    ["Mouse", "Keyboard", "Monitor", "Webcam"],
    ["Hamster", "Gerbil", "Chinchilla", "Guinea Pig"]];
  final.practiceGroups = _.shuffle(final.practiceGroups);
  for (var i = 0; i < final.practiceGroups.length; i++) {
    final.practiceGroups[i] = _.shuffle(final.practiceGroups[i]);
  }

  // set up experimental params
  final.experimentParams = {};

  // selections: 8 items per menu, weighted as 15, 8, 5, 4, 3, 3, 2, 2 (= 42) ("Zipfian")
  var EASY_MODE = false;
  var copies = EASY_MODE ? [1, 1] : [15, 8, 5, 4, 3, 3, 2, 2];
  var selections = [];
  for (var m = 0; m < 3; m++) {
    var picked = _.shuffle(_.range(16));
    for (var i = 0; i < copies.length; i++) {
      selections = selections.concat(Array(copies[i]).fill(picked[i] + m * 16));
    }
  }
  final.experimentParams.selections = _.shuffle(selections);
  final.experimentParams.conditions = _.shuffle(["Baseline", "Ephemeral"]);

  final.begin = function() {
    if (window.location.hash.indexOf('skip') != -1) {
      final.preparePracticeExperiment();
    } else if (window.location.hash.indexOf('baseline') != -1) {
      final.experimentParams.conditions = ["Baseline"];
      final.finishPracticeExperiment();
    } else if (window.location.hash.indexOf('ephemeral') != -1) {
      final.experimentParams.conditions = ["Ephemeral"];
      final.finishPracticeExperiment();
    } else if (window.location.hash.indexOf('survey') != -1) {
      final.finishMainExperiment(null);
    } else {
      $('#begin-modal').modal(final.modalOptions);
      $('#submit-begin').on("click", function() {
        final.prepareDemographicsForm();
      });
    }
  };
  
  final.prepareDemographicsForm = function() {
    $('#begin-modal').modal('hide');
    $('#demographics-modal').modal(final.modalOptions);
    $("#demographics-submit").on("click", function() {
      final.submitDemographicsForm();
    });
  };
  
  final.submitDemographicsForm = function() {
    $("#demographics-modal").modal('hide');

    $.ajax({
      'type': "POST",
      'url': '/demographics',
      'data': {
        'gender': $("input[name=gender]").val(),
        'age': $("input[name=age]").val(),
        'education': $("select[name=education]").val(),
        'pointer': $("select[name=pointer]").val(),
        'handedness': $("select[name=handedness]").val(),
        'language': $("input[name=language]").val(),
        'experience': $("select[name=experience]").val()
      },
      'success': function() {
        final.preparePracticeModal();
      }
    });
  };

  final.preparePracticeModal = function() {
    $("#practice-modal").modal(final.modalOptions);

    $("#submit-begin-experiment").off(".begin").on("click.begin", function() {
      final.preparePracticeExperiment();
    });
  };

  final.preparePracticeExperiment = function() {
    console.log("preparePracticeExperiment");
    $("#practice-modal").modal('hide');
    $("#intermission-modal").modal('hide');
    $("#feedback-modal").modal('hide');

    var fadeIn;
    if (final.experimentParams.conditions[0] === "Baseline") {
      fadeIn = false;
    } else {
      fadeIn = true;
    }

    // construct experiment object
    var items = _.flatten(final.practiceGroups);
    var trials = _.shuffle(_.range(48)).slice(0, EASY_MODE ? 2 : 8); // just pick 8 menu items

    var exp = new final.Experiment(items, trials, fadeIn, final.finishPracticeExperiment);
    exp.install();

    $("#experiment").show();
  };

  final.finishPracticeExperiment = function(exp) {
    console.log("finishPracticeExperiment");
    $("#experiment").hide();

    if (final.experimentParams.conditions.length === 1) {
      $("#intermission-modal-title").text("Almost finished!");
    }
    $("#intermission-modal").modal(final.modalOptions);

    $("#submit-intermission").off(".intermission").on("click.intermission", function() {
      final.prepareMainExperiment();
    });
  };

  final.prepareMainExperiment = function() {
    console.log("prepareMainExperiment");
    $("#intermission-modal").modal('hide');

    var fadeIn;
    if (final.experimentParams.conditions[0] === "Baseline") {
      fadeIn = false;
    } else {
      fadeIn = true;
    }

    // construct experiment object
    var items = _.flatten(final.groups.slice(0, 12));
    final.groups = final.groups.slice(12); // jankily cut off the used groups
    var trials = final.experimentParams.selections.slice(0);

    var exp = new final.Experiment(items, trials, fadeIn, final.finishMainExperiment);
    exp.install();

    $("#experiment").show();
  };
  
  final.finishMainExperiment = function(exp) {
    console.log("finishMainExperiment");
    console.log(exp);

    // send the experimental results off to the server
    if(exp){
      $.ajax({
        'type': "POST",
        'url': '/taskdata',
        'contentType': "application/json; charset=utf-8",
        'dataType': "json",
        'data': JSON.stringify( exp )
      });
    }

    $("#experiment").hide();
    $("#feedback-modal").modal(final.modalOptions);

    $("#submit-feedback").off(".feedback").on("click.feedback", function() {
      $.ajax({
        'type': "POST",
        'url': '/feedback',
        'data': {
          'difficulty': $("input[name=difficulty]:checked").val(),
          'satisfaction': $("input[name=satisfaction]:checked").val(),
          'frustration': $("input[name=frustration]:checked").val(),
          'efficiency': $("input[name=efficiency]:checked").val()
        },
        'success': function() {
          // clear radio buttons
          console.log("CLEARING");
          $("input[name=difficulty]:checked").prop("checked", false);
          $("input[name=satisfaction]:checked").prop("checked", false);
          $("input[name=frustration]:checked").prop("checked", false);
          $("input[name=efficiency]:checked").prop("checked", false);

          // show next experiment if a condition remains
          final.experimentParams.conditions = final.experimentParams.conditions.slice(1);
          if (final.experimentParams.conditions.length === 0)
            final.prepareGoodbye();
          else
            final.preparePracticeExperiment();
        }
      });
    });
  };

  final.prepareGoodbye = function() {
    $("#feedback-modal").modal("hide");
    $("#goodbye-modal").modal(final.modalOptions);
  };

  /* expected arguments:
   * items - a flat list of 48 items representing 12 semantically related groups of 4
   * selections - order to prompt item selection (will be obfuscated by menu permutation)
   * fadeIn - experimental condition
   * finishHook - hook to call when done with experiment */
  final.Experiment = function(items, selections, fadeIn, finishHook) {
    var mkMenu = function(menuNum, dropdown) {
      return _.template('\
        <div class="experiment-menu">\
        <div id="experiment-menu-button-<%= menuNum %>" class="experiment-menu-button">Menu <%= menuNum %></div>\
        <%= dropdown %>\
        </div>')({menuNum: menuNum, dropdown: dropdown});
    };

    var mkMenuDropdown = function(menuNum, options) {
      return _.template('\
        <div id="experiment-menu-dropdown-<%= menuNum %>" class="experiment-menu-dropdown" style="display:none;">\
        <%= options %>\
        </div>')({menuNum: menuNum, options: options});
    };

    var mkMenuOption = function(menuNum, optionNum, text) {
      return _.template('\
        <div id="experiment-menu-<%= menuNum %>-option-<%= optionNum %>" class="experiment-menu-option">\
        <span class="menu-option-text"><%= text %></span>\
        </div>')({menuNum: menuNum, optionNum: optionNum, text: text});
    };

    // construct menu HTML
    var menuElements = "";
    var menuItems = items.slice(0);
    this.menuItems = []; // save item order
    for (var m = 1; m <= 3; m++) {
      var options = "";
      for (var o = 1; o <= 16; o++) {
        var nextItem = menuItems.pop();
        this.menuItems.push(nextItem);
        options += mkMenuOption(m, o, nextItem);
      }

      menuElements += mkMenu(m, mkMenuDropdown(m, options));
    }
    this.menuElements = menuElements;

    // ****************************
    // * SELECTION HANDLING LOGIC *
    // ****************************

    // pre-select 80% of trials for correct fade-ins
    var numCorrectTrials = Math.floor(selections.length * 0.8);
    var numIncorrectTrials = selections.length - numCorrectTrials;
    this.fadeCorrect = _.shuffle(Array(numCorrectTrials).fill(true)
      .concat(Array(numIncorrectTrials).fill(false)));

    this.fadeDecided = this.fadeCorrect.slice(0);

    // pick a random menu permutation
    var menuPerm = _.shuffle(_.range(3));

    // get [menu, opt] intended by current selection
    this.selections = selections.slice(0);
    this.getSelection = function() {
      var menu = menuPerm[Math.floor(this.selections[0] / 16)];
      var opt = this.selections[0] % 16;

      return [menu, opt];
    };

    this.showTrial = function() {
      var loc = this.getSelection();

      // set prompt
      var promptText = _.template('Menu <%= loc %> > <%= menuItems %>')({
        loc: loc[0] + 1,
        menuItems: this.menuItems[loc[0]*16 + loc[1]]
      });
      $("#experiment-prompt").text(promptText);

      var menuOptionId = function(m, o) {
        return _.template('#experiment-menu-<%= menu %>-option-<%= opt %> .menu-option-text')({
          menu: m,
          opt: o
        });
      };

      // set fade-ins
      if (fadeIn) {
        $(".experiment-menu-option .menu-option-text").addClass("fade-in");

        if (this.fadeCorrect[0]) {
          // unfade correct item
          $(menuOptionId(loc[0]+1, loc[1]+1)).removeClass("fade-in");

          // unfade two incorrect in same menu
          var wrong = _.shuffle(_.range(16));
          var unfaded = 0;
          for (var o = 0; unfaded < 2; o++) {
            if (wrong[o] !== loc[1]) {
              $(menuOptionId(loc[0]+1, wrong[o]+1)).removeClass("fade-in");
              unfaded++;
            }
          }
          
          // unfade three in each other menu
          for (var m = 0; m < 3; m++) {
            if (m !== loc[0]) {
              var wrong = _.shuffle(_.range(16));
              for (var o = 0; o < 3; o++) {
                $(menuOptionId(m+1, wrong[o]+1)).removeClass("fade-in");
              }
            }
          }
        } else {
          // select three incorrect items per menu
          for (var m = 0; m < 3; m++) {
            var wrong = _.shuffle(_.range(16));
            var unfaded = 0;
            for (var o = 0; unfaded < 3; o++) {
              if (m !== loc[0] || wrong[o] !== loc[1]) {
                $(menuOptionId(m+1, wrong[o]+1)).removeClass("fade-in");
                unfaded++;
              }
            }
          }
        }
      }
    };

    this.nextTrial = function() {
      this.finishTrial();

      this.selections = this.selections.slice(1);
      this.fadeCorrect = this.fadeCorrect.slice(1);

      if (this.selections.length === 0) {
        // pack up the measurements and pass out to the hook
        var exp = {
          "condition": fadeIn ? "Ephemeral" : "Baseline",
          "permutation": menuPerm,
          "selection": selections,
          "trials": this.trials,
          "fadeIns": this.fadeDecided
        };

        finishHook(exp);
      } else {
        this.showTrial();
      }
    }

    // **********************
    // * TRIAL DATA LOGGING *
    // **********************

    this.trials = [];
    this.curTrial = {
      "correct" : true,
      "expected" : this.getSelection(),
      "events" : []
    };

    this.finishTrial = function () {
      this.trials.push(this.curTrial);
      this.curTrial = {
        "correct" : true,
        "expected" : this.getSelection(),
        "events" : []
      };
    };

    this.trialSetIncorrect = function () {
      this.curTrial["correct"] = false;
    };

    this.trialLogMenuButtonEvent = function(menuNum, timestamp) {
      var e = {
        "type" : "button",
        "timestamp" : timestamp,
        "menuNum" : menuNum
      };

      this.curTrial.events.push(e);
    };

    this.trialLogMenuOptionEvent = function(menuNum, optionNum, timestamp) {
      var e = {
        "type" : "option",
        "timestamp" : timestamp,
        "menuNum" : menuNum,
        "optionNum" : optionNum
      };

      this.curTrial.events.push(e);
    };

    // ******************************
    // * DISPLAY AND EVENT HANDLING *
    // ******************************

    // handle menu clicks
    this.menuButtonClicked = function(menuNum, timestamp) {
      this.trialLogMenuButtonEvent(menuNum, timestamp);
    };

    // handle option clicks
    this.menuOptionClicked = function(menuNum, optionNum, timestamp) {
      this.trialLogMenuOptionEvent(menuNum, optionNum, timestamp);

      // advance to next trial if appropriate
      var loc = this.getSelection();
      if (menuNum - 1 === loc[0] && optionNum - 1 === loc[1]) {
        this.nextTrial();
      } else {
        this.trialSetIncorrect();
      }
    };

    this.install = function() {
      console.log("NOW INSTALLING NEW EXPERIMENT");

      var instance = this;

      // clear current menus, drop constructed menus
      $(".experiment-menubar").empty();
      $(".experiment-menubar").append(this.menuElements);

      // update prompt text and menu fade-ins
      this.showTrial();

      // install measurement handlers
      $(".experiment-menu-button").on("click", function (e) {
        m = /experiment-menu-button-(\d+)/.exec($(this).attr("id"));
        // XXX timeStamp is NOT supported in Firefox
        // XXX timeStamp in Chrome is time since page loaded
        instance.menuButtonClicked(m[1], e.timeStamp);
      });
      $(".experiment-menu-option").on("click", function (e) {
        m = /experiment-menu-(\d+)-option-(\d+)/.exec($(this).attr("id"));
        instance.menuOptionClicked(m[1], m[2], e.timeStamp);
      });

      // menu show/hide functionality
      $("#experiment-menu-button-1").on("click", function() {
        $("#experiment-menu-dropdown-1").toggle();
        $("#experiment-menu-dropdown-2").hide();
        $("#experiment-menu-dropdown-3").hide();
        $(".fade-in").hide().fadeIn(500);
      });
      $("#experiment-menu-button-2").on("click", function() {
        $("#experiment-menu-dropdown-1").hide();
        $("#experiment-menu-dropdown-2").toggle();
        $("#experiment-menu-dropdown-3").hide();
        $(".fade-in").hide().fadeIn(500);
      });
      $("#experiment-menu-button-3").on("click", function() {
        $("#experiment-menu-dropdown-1").hide();
        $("#experiment-menu-dropdown-2").hide();
        $("#experiment-menu-dropdown-3").toggle();
        $(".fade-in").hide().fadeIn(500);
      });
      $(".experiment-menu-option").on("click", function () {
        $("#experiment-menu-dropdown-1").hide();
        $("#experiment-menu-dropdown-2").hide();
        $("#experiment-menu-dropdown-3").hide();
      });
    };
  };

  final.begin();
});