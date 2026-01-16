var wavesurfer;
window.keyframes;

$(document).ready(function() {

  wavesurfer = WaveSurfer.create({
    container: '#waveform',
    waveColor: 'violet',
    progressColor: 'purple',
    // normalize: true,
    pixelRatio: 1,
    height: 256
  });
  wavesurfer.load('UclCCFNG9q4.mp3');

  wavesurfer.on('ready', function () {

    var split = wavesurfer.getDuration() / 1024,
      pcm = JSON.parse(wavesurfer.exportPCM(1024,10000,true)),
      c= $("#canvas")[0],
      ctx=c.getContext("2d"),
      peak = 0,
      markers = 0;
    
    window.keyframes = [];

    for (var i = 0; i < pcm.length; i++) {
      x = Math.floor(i/2);
      peak = -pcm[i]*128;

      // find volume peaks, but need to find percussion/bassline peaks
      if ((i > 4) && (Math.abs(peak) - Math.abs(-pcm[i-2]*128) > 20)) {
        ctx.fillStyle = "red";
        window.keyframes.push( (split * x).toFixed(2) );
      } else {
        ctx.fillStyle = "black";
      }
      ctx.fillRect(x,50,1,peak);
    }

    console.log(window.keyframes);


    wavesurfer.play();

  });

  wavesurfer.on('audioprocess', function (time) {

    if ( $.inArray(time.toFixed(2), window.keyframes) != -1 ) {
      console.log(time);
    }
  });

});