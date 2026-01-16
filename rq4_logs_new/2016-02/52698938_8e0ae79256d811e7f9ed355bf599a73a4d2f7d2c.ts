requirejs.config({
    "baseUrl": "scripts/app",
    "paths": {
      "jquery": "https://ajax.googleapis.com/ajax/libs/jquery/2.2.0/jquery.min",
      "easel": "https://code.createjs.com/easeljs-0.8.2.min",
      // "tween": "https://code.createjs.com/tweenjs-0.6.2.min",
      "sound": "https://code.createjs.com/soundjs-0.6.2.min",
      "preload": "https://code.createjs.com/preloadjs-0.6.2.min",
    },

    "shim": {
        "easel": {
            "exports": 'createjs'
        },
        // "tween": {
        //     "deps": ['easel'],
        //     "exports": 'Tween'
        // }
    }
});

requirejs(["Main"]);