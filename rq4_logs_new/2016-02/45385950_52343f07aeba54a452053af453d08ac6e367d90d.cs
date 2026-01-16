using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using AntWars.AIs.Converter.Classes;
using AntWars.Exception;

namespace AntWars.AIs
{
    public class AI
    {
        private static String CLASS_PLAYERAI = "PlayerAI.AI";
        private static String METHOD_ANT_TICK = "antTick";
        private static String METHOD_NEXT_TICK = "nextTick";
        private static String PROPERTY_GAME = "Game";
        private static String PROPERTY_PLAYER = "Player";

        private Type playerAI;
        public object instance;

        public AI(String path, Game game, Player player)
        {
            try
            {
                Assembly DLL = Assembly.LoadFile(path);
                playerAI = DLL.GetType(CLASS_PLAYERAI);
                instance = Activator.CreateInstance(playerAI);

                setProperty(PROPERTY_PLAYER, player);
                setProperty(PROPERTY_GAME, game);
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }
        }

        public void setProperty(string name, object value)
        {
            PropertyInfo playerProperty = playerAI.GetProperty(name);
            playerProperty.SetValue(instance, value, null);
        }

        public void antTick(AIAnt ant, List<AIBoardObject> view)
        {
            try
            {
                playerAI.InvokeMember(METHOD_ANT_TICK, BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public, null, instance, new Object[] { ant, view });
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }
        }

        public void nextTick()
        {
            try
            {
                playerAI.InvokeMember(METHOD_NEXT_TICK, BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public, null, instance, new Object[0]);
            }
            catch (System.Exception e)
            {
                throw new InvalidDLLFileException();
            }
        }
    }
}