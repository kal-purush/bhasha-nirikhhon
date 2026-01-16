$(document).ready(function () {
  const playSound = (name) => {
    name.pause();
    name.currentTime = 0;
    name.play();
  };

  $(document).on("click", ".pads", (e) => {
    let sample = $(`#${e.target.id} audio`)[0];
    playSound(sample);
  });

  $(document).on("keypress", (e) => {
    let key = e.key.toLowerCase();
    let keyTrigger = $(`.${key} audio`)[0];
    playSound(keyTrigger);
  });
});