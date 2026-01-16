$(document).ready(function() {
  var synth = new Tone.PolySynth(6, Tone.Synth).toMaster(),
      loop;

  function stop () {
    Tone.Transport.stop();
  }

  function start () {
    Tone.Transport.start();
  }

  $('form').submit(function(e) {
    e.preventDefault();

    stop();

    if ( loop ) {
      loop.dispose();
    }

    $.getJSON('/api/notes', {
      inputNotes: [ $('#fq1').val(), $('#fq2').val() ]
    }).done(function(response) {
      console.log(response);

      let seq = new Tone.Sequence(function(time, pitch) {
        synth.triggerAttackRelease(pitch, '4n', time);
      }, response.notes, '4n');

      seq.start(0);
      seq.loop = true;
      seq.humanize = true;
      loop = seq;

      Tone.Transport.bpm.value = response.tempo;
      start();
    });
  });

  $('#start').click(start);
  $('#stop').click(stop);
});