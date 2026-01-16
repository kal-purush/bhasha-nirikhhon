const MAX_PATIENCE = 10;
let strikes: number[];

function reset() : void {
    strikes = [];
    light.setAll(Colors.Black);
}

function displayPatience(): void {
    light.setAll(Colors.Black);
    for (let i = 0; i < strikes.length; i++) {
        light.setPixelColor(i, Colors.Red);
    }
}

input.onLoudSound(() => {
    if (strikes.length < MAX_PATIENCE) {
        strikes.push(input.soundLevel());
        // Sum of everything
        const allLevels = strikes.reduce((prev, current) => prev + current, 0);
        music.playTone(allLevels, music.beat(BeatFraction.Half));
        displayPatience();
    } else {
        music.playSound(music.sounds(Sounds.Siren));
        light.showAnimation(light.theaterChaseAnimation, 5000);
    }
});

input.buttonB.onEvent(ButtonEvent.Click, reset);

reset();

