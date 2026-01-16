using System;
using System.Collections.Generic;
using System.Linq;
using System.Media;
using System.Text;
using System.Threading.Tasks;

namespace Snake.Classes
{
    public class GameMenu
    {
        #region DIFFICULTY LEVEL
        public int Level { get; set; }
        #endregion

        #region OWN SETTINGS
        public bool OwnSettings { get; set; }
        public int SnakeSpeed { get; set; }
        public int BonusFruit { get; set; }
        #endregion

        #region MUSIC
        private SoundPlayer SoundPlayer;

        //public string LastPath { get; set; }
        public string MusicPath { get; set; }
        public bool MusicOn { get; set; }
        #endregion

        public GameMenu() { }

        public GameMenu(int Level, bool OwnSettings, int SnakeSpeed, int BonusFruit, string MusicPath, bool MusicOn)
        {
            this.Level = Level;
            this.OwnSettings = OwnSettings;
            this.SnakeSpeed = SnakeSpeed;
            this.BonusFruit = BonusFruit;
            this.MusicPath = MusicPath;
            this.MusicOn = MusicOn;
        }

        public void PlayMusic()
        {
            SoundPlayer = new SoundPlayer(MusicPath);
            SoundPlayer.PlayLooping();
            MusicOn = true;
        }

        public void StopMusic()
        {
            SoundPlayer.Stop();
            MusicOn = false;
        }
    }
}