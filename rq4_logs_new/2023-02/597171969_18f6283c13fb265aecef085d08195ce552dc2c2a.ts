export class Sound {
  play: () => void;
  sound: HTMLAudioElement;
  stop: () => void;

  constructor({ src }: { src: string }) {
    this.sound = document.createElement("audio");
    this.sound.src = src;
    this.sound.setAttribute("preload", "auto");
    this.sound.setAttribute("controls", "none");
    this.sound.style.display = "none";
    document.body.appendChild(this.sound);
    this.play = function () {
      this.sound.play();
    };
    this.stop = function () {
      this.sound.pause();
    };
  }
  function() {
    this.sound.play();
  }
}