using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Aerial_Mayhem
{
    public  enum GameScenes{
    StartScreen,OptionScreen,CharacterSlector,LevelSelector,Pause,GameOver,
    }
    abstract class GameScenes
    {
        public void CahngeScene(GameScenes scene);
    }
}