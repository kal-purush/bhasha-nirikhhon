
const Notifier = require("node-notifier");

var messages = [
  "Whoa stop working so hard...",
  "Them tities!",
  "Crookers Knobbers rocks",
  "Spektrum der wissenschaft",
  "Where is Elminster???",
  "Ever heard of Friendship is Manly?"
];

var sounds = [
  "Basso",
  "Blow",
  "Bottle",
  "Frog",
  "Funk",
  "Glass",
  "Hero",
  "Morse",
  "Ping",
  "Pop",
  "Purr",
  "Sosumi",
  "Submarine",
  "Tink"
];

var pick = (array) => array[Math.floor(Math.random() * array.length)];

module.exports = () =>
  Notifier.notify({
    title: "Volothamp",
    message: pick(messages),
    contentImage: __dirname+"/volo.png",
    sound: pick(sounds)
  });