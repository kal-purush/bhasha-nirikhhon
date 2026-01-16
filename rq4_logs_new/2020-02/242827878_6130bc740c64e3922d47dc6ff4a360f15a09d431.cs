using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace HuntTheWumpus
{
    class UI
    {
        //private Texture2D playerTexture;
        //private Texture2D backgroundTexture; 
        //i will add the rest of the textures later

        public UI(/*textures will be passed in here*/) //this class will draw things on the screen. when a method is called it will draw something and layer it on top of the last thing to be drawn
        {
            //this.playerTexture = playerTexture
            //again, i will add the rest of the textures later
        }

        public void drawPlayer(int playerX, int playerY, bool facingRight) //draws the player, in the future will need to be given the state of the player (walking? jumping? etc)
        {

        }
        public void drawRoom(int[] roomNumbers, int numberOfDoors) //draws the room, since its the furthest in the background will need to be called first before drawing anything else
        {

        }
        public void drawHole() //draws the bottomless pit
        {

        }
        public void drawTrivia(String question, String lettersTyped) //displays the trivia question and a box to type answers. displays what has been typed but doesnt handle if its correct
        {

        }
        public void drawMap() //displays the map of the cave, will need to be called last because its the furthest in the forground
        {

        }
        public void drawMessage(String message) //displays messages like "i smell a wumpus"
        {

        }
        public void drawStats(int arrows, int health, int score, int coins, Label scoreOutput) //displays the statistics of the player
        {
            String scoreStr = score.ToString();
            scoreOutput.Text = scoreStr;
        }
        public void drawStartScreen() //draws the start menu
        {

        }
        public void drawEndScreen(int score, Boolean win) //draws the credits if win == true, and the death screen if win == false
        {

        }
        public void drawHelpScreen() //draws the help screen/how to play screen
        {

        }
        public void drawPauseMenue() //to be determined what will be displayed
        {

        }
    }
}