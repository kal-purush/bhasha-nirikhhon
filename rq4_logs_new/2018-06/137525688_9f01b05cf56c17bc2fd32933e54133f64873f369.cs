using System;
using System.IO;
using System.Media;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SoundTest
{
    /// <summary>
    /// Class for performing audio playback tasks.
    /// </summary>
    class PlaybackManager
    {
        private string AudioPath;
        private SoundPlayer SoundPlayer;

        public PlaybackManager(string audioFilePath)
        {
            SoundPlayer = new SoundPlayer();
            AudioPath = audioFilePath;
        }

        /// <summary>
        /// Method which starts playing specified sound file in a separate thread.
        /// </summary>
        /// <returns>True if sound playback start was successful, otherwise returns False.</returns>
        public bool PlaySound()
        {
            try
            {
                // 30s should hopefully cover loading of most WAV files.
                // Definitely way more than enough for a file with 10s of sound in it at 44kHz.
                Console.WriteLine("Attempting to load specified sound file.");
                SoundPlayer.LoadTimeout = 30000;
                SoundPlayer.SoundLocation = AudioPath;
                SoundPlayer.LoadAsync();
                Console.WriteLine("Sound file load completed.");
                Console.WriteLine("Attempting to start sound playback.");
                SoundPlayer.PlayLooping();
                Console.WriteLine("Sound playback started successfully.");
                return true;
            }
            catch (TimeoutException)
            {
                Console.Error.WriteLine("Loading specified file took longer than allowed timeout of "
                                        + SoundPlayer.LoadTimeout.ToString() + "ms.");
                return false;
            }
            catch (InvalidOperationException)
            {
                Console.Error.WriteLine("Specified playback file is not a valid sound file.");
                return false;
            }
        }

        /// <summary>
        /// Method which stops playback of specified sound file.
        /// </summary>
        /// <returns>True if sound playback was terminated successfully, otherwise returns false.</returns>
        public bool StopSound()
        {
            try
            {
                Console.WriteLine("Attempting to stop sound playback.");
                SoundPlayer.Stop();
                Console.WriteLine("Sound playback stop performed successfully.");
                return true;
            }
            catch (System.Exception e)
            {
                // No idea why this function could throw an exception, but just in case, to aid debugging...
                Console.Error.WriteLine("Unexpected error occurred while attempting to stop sound playback: " + e.Message);
                return false;
            }
        }
    }
}