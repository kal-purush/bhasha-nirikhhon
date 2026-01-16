input.onButtonPressed(Button.A, function () {
    for (let index = 0; index < 4; index++) {
        music.play(music.stringPlayable("C5 A B G A F G E ", 220), music.PlaybackMode.UntilDone)
    }
})
input.onButtonPressed(Button.AB, function () {
	
})
input.onButtonPressed(Button.B, function () {
    music.play(music.stringPlayable("B A G A G F A C5 ", 150), music.PlaybackMode.UntilDone)
})