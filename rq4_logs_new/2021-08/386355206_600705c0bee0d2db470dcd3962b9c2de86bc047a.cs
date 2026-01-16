using System.Linq;

namespace RandomKatas
{
    public class AlanPartridgeOnePartridgeWatch
    {
        /* Given an array of terms, if any of those terms relate to Alan Partridge, return Mine's a Pint!
            The number of ! after the t should be determined by the number of Alan related terms you find
            in the provided array (x). 
            The related terms are:
            Partridge
            PearTree
            Chat
            Dan
            Toblerone
            Lynn
            AlphaPapa
            Nomad
            If you don't find any related terms, return 'Lynn, I've pierced my foot on a spike!!'
            */
        public static string Part(string[] x)
        {
            string[] terms = {"Partridge", "PearTree", "Chat", "Dan", "Toblerone", "Lynn", "AlphaPapa", "Nomad"};
            var count = x.Count(s => terms.Contains(s)); // чекнуть
            return count > 0 ? "Mine's a Pint" + new string('!', count) : "Lynn, I've pierced my foot on a spike!!";
        }
    }
}