console.log("EMPIRE | Loading");

Hooks.once("init", function() {
    console.log("EMPIRE | Init");
});

Hooks.on("ready", function() {
    console.log("Empire | Ready");
});