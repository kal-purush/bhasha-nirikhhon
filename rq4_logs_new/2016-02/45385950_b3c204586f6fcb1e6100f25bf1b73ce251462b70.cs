using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using AntWars.Exception;
using AntWars.Board;
using AntWars.Board.Ants;

namespace AntWars.AI
{
    class AILoader
    {
        private static String CLASS_PLAYERAI = "PlayerAI.AI";
        private static String CLASS_ANTAI = "PlayerAI.AIAnt";

        private Type playerAI;
        private Type antAI;

        public AILoader(String path)
        {
            Assembly DLL = null;
            try
            {
                DLL = Assembly.LoadFile(path);
                playerAI = DLL.GetType(CLASS_PLAYERAI);
                antAI = DLL.GetType(CLASS_ANTAI);
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }
            if (antAI == null || playerAI == null || 
                playerAI.BaseType != typeof(AIBase) || antAI.BaseType != typeof(AIAntBase))
            {
                throw new InvalidDLLFileException();
            }
        }

        public IAI createAIInstance(Game game, Player player)
        {
            try
            {
                AIBase obj = (AIBase)Activator.CreateInstance(playerAI);
                obj.Game = game;
                obj.Player = player;
                return obj;
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }

        }

        public IAIAnt createAIAntInstance(Ant ant)
        {
            try
            {
                AIAntBase obj = (AIAntBase)Activator.CreateInstance(antAI);
                obj.Ant = ant;
                return obj;
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }
        }

    }
}