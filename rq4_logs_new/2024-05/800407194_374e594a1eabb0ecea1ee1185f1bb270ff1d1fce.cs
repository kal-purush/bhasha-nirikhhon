using Chess;
using NAudio.Wave;
using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ChessAI_Bck
{
    public class SoundFXHandler : IDisposable
    {
        private IWavePlayer waveOutDevice;
        private AudioFileReader audioFileReader;
        private string filePath;
        private readonly string soundDirectory;
        private bool disposed = false;

        private readonly Dictionary<string, string> soundFX = new Dictionary<string, string>
        {
            { "move", "move-self.mp3" },
            { "capture", "capture.mp3" },
            { "check", "move-check.mp3" },
            { "draw", "game-draw.mp3" },
            { "win", "game-win.mp3" },
            { "lose", "game-lose.mp3" },
            { "end", "game-end.mp3" },
            { "start", "game-start.mp3" },
            { "castle", "castle.mp3" },
            { "promote", "promote.mp3" },
            { "invalid", "illegal.mp3" },
            { "tenSec", "tenseconds.mp3" },
            { "decline", "decline.mp3" },
            { "accept", "correct.mp3" },
            { "drawOffer", "drawoffer.mp3" },
            { "Offer", "drawoffer.mp3" },
            { "click", "click.mp3" },
        };

        public SoundFXHandler(ChessBoard chessBoard, string moveSAN, string ovrdSoundCode = "", bool isDebug = true, PieceColor side = null )
        {
            soundDirectory = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory())?.Parent?.Parent?.FullName ?? "", "SoundFX");

            string fileName = DetermineSoundFileName(chessBoard, moveSAN, ovrdSoundCode, side);
            if (string.IsNullOrEmpty(fileName))
            {
                return;
            }
            filePath = Path.Combine(soundDirectory, fileName);

            if (!File.Exists(filePath))
            {
                Debug.WriteLineIf(isDebug,$"Sound file not found: {filePath}");
                return;
            }
          

            Debug.WriteLineIf(isDebug,fileName);
            PlayAndDispose();
        }

        private string DetermineSoundFileName(ChessBoard chessBoard, string moveSAN, string ovrdSoundCode, PieceColor side)
        {
            
            if (!string.IsNullOrEmpty(ovrdSoundCode) && soundFX.ContainsKey(ovrdSoundCode))
            {
                return soundFX[ovrdSoundCode];
            }
            if(chessBoard == null)
            {
                Debug.WriteLine("Chessboard is null");
                return soundFX["invalid"];
            }


            var currentTurn = chessBoard.Turn;
            if (chessBoard.IsEndGame)
            {
                if (chessBoard.EndGame.EndgameType == EndgameType.DrawDeclared)
                {
                    return soundFX["draw"];
                }
                return chessBoard.EndGame.WonSide == side ? soundFX["win"] : soundFX["lose"];
            }

            if (chessBoard.BlackKingChecked || chessBoard.WhiteKingChecked)
            {
                return soundFX["check"];
            }

            if (string.IsNullOrEmpty(moveSAN) && string.IsNullOrEmpty(ovrdSoundCode))
            {
                return "";
            }

            if (chessBoard[moveSAN] != null && chessBoard[moveSAN].Color == currentTurn)
            {
                return soundFX["castle"];
            }

            if (chessBoard[moveSAN] != null&& side != null && chessBoard[moveSAN].Color != side)
            {
                return soundFX["capture"];
            }

            return soundFX["move"];
        }

        public void PlayAndDispose()
        {
            try
            {
                waveOutDevice = new WaveOutEvent();
                audioFileReader = new AudioFileReader(filePath);

                waveOutDevice.Init(audioFileReader);
                waveOutDevice.PlaybackStopped += OnPlaybackStopped;
                waveOutDevice.Play();
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Error playing sound: " + ex.Message);
                Dispose();
            }
        }

        private void OnPlaybackStopped(object sender, StoppedEventArgs e)
        {
            Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    waveOutDevice?.Stop();
                    waveOutDevice?.Dispose();
                    audioFileReader?.Dispose();
                }

                waveOutDevice = null;
                audioFileReader = null;
                disposed = true;
            }
        }

        ~SoundFXHandler()
        {
            Dispose(false);
        }
    }
}