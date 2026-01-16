using System;

namespace Quest
{
    public class Hat 
    {
        public static int ShininessLevel { get; set; }

        public static string ShininessDescription 
        {
            get {
                if (ShininessLevel <2 )
                {
                    return "Dull!";
                }
                else if (ShininessLevel >= 2 && ShininessLevel <= 5)
                {
                    return "Noticeable...";
                }
                else if (ShininessLevel >=6 && ShininessLevel <= 9)
                {
                    return "B R I G H T";
                }
                else
                {
                    return "!BLINDING!";
                }
            }
        }
    }
}