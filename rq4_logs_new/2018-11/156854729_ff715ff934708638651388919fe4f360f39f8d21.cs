using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestEngine {
    class HighScore {

        public List<Int32> scores;
        private List<Text> scoresText;
        public HighScore(List<Int32> scores) {
            this.scores = scores;
            scoresText = new List<Text>();
            loadScoresText();
        }

        public void input() {
            if (Game.GetKey(Keys.ESCAPE)) Program.changeState(Program.States.MainMenu);
        }

        public void update(List<Int32> newScores) {
            if (newScores != scores) {
                scores = newScores;
                loadScoresText();
                Save.SaveBytesFile(scores);
            }
        }

        public void render() {
            foreach (Text scoreText in scoresText) {
                scoreText.drawText();
            }
        }

        private void loadScoresText() {
            scoresText = new List<Text>();
            int i = 1;
            float x = 260;
            float y = 30;
            foreach (Int32 score in this.scores) {
                if (i >= 10) {
                    x -= 31;
                }
                Text text = new Text(i.ToString() + ". " + Utils.Truncate("00000000" + score.ToString(), 8),x,y,30,35);
                scoresText.Add(text);
                y += 60;
                i++;
            }
            
        }

    }
}