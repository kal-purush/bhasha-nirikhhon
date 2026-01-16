using Microsoft.AspNetCore.Mvc;
using Commons.Music.Midi;

namespace Limidi_Desktop.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class MIDI_InputController : ControllerBase
    {
        static private string _midi_output_device_id = "2";
        /*
        TODO : Make a GET endpoint that lists out all of the Midi output devices that are currently visible to the desktop server
        TODO : Make a PUT endpoint that allows the mobile app to set the above static variable based on the response from the above TODO GET endpoint
        TODO : Add a midi device connection lost error handling to GetMIDI_Input. Mobile side should start to try to reconect
         */



        [HttpGet(Name = "GetMIDI_Input")]
        public IEnumerable<String> GetMIDI_Input(
            [FromQuery(Name = "isNoteOn")] bool isNoteOn, 
            [FromQuery(Name = "note")] string note, // Must be natural or flat. Sharps are strange as query params
            [FromQuery(Name = "octave")] int octave,// Must by between 0 & 10 inclusive
            [FromQuery(Name = "velocity")] int velocity // Must by between 1 & 127 inclusive
        )
        {
            var access = MidiAccessManager.Default;
            // var output = access.OpenOutputAsync(access.Outputs.Last().Id).Result; // OG dynamically setting the output device
            var output = access.OpenOutputAsync(_midi_output_device_id).Result; 
   
            
            
            output.Send(
                this.createMidiNoteByteArray(isNoteOn, note, octave, velocity), //Note itself
                0, // Offset
                3, // Length. Always 3 bytes
                0 // Timestamp
            );

            //Cleanup and respond
            output.CloseAsync();
            return new string[] { "ok" };
        }

        private byte[] createMidiNoteByteArray(bool isNoteOn, string noteLetter, int octave, int velocity) 
        {
            NoteOffsets.TryGetValue(noteLetter, out int noteOffset);
            return new byte[] {
                isNoteOn ? MidiEvent.NoteOn : MidiEvent.NoteOff,
                (byte) (12 * octave + noteOffset), //Note number, i.e C5, A#7
                (byte) velocity // AKA Volume
            };
        }

        private static readonly Dictionary<string, int> NoteOffsets = new Dictionary<string, int>
        {
            {"C",  0},
            {"Db", 1},
            {"D",  2},
            {"Eb", 3},
            {"E",  4},
            {"F",  5},
            {"Gb", 6},
            {"G",  7},
            {"Ab", 8},
            {"A",  9},
            {"Bb", 10},
            {"B",  11}
        };

      
    }
}