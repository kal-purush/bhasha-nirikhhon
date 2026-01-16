using System;

namespace VikingsPiratesNinjas
{
    public class PlayerSelection
    {
        public Selection UserSelection()
        {
            string selection = Console.ReadLine();

            switch (selection)
            {
                //Erm not sure about the case selects.
                case PlayGame.Ninja:
                    return Selection.Ninja;
                case "n":
                    return Selection.Ninja;
                case PlayGame.Pirate:
                    return Selection.Pirate;
                case "p":
                    return Selection.Pirate;
                case PlayGame.Viking:
                    return Selection.Viking;
                case "v":
                    return Selection.Viking;
                case "e":
                    return Selection.Exit;
                case PlayGame.Exit:
                    return Selection.Exit;
                default:
                    return Selection.Viking;
            }
        }

        public Selection PcSelection()
        {
            Random rnd = new Random();
            int choice = rnd.Next(1, 4);

            switch (choice)
            {
                case 1:
                    return Selection.Pirate;
                case 2:
                    return Selection.Ninja;
                case 3:
                    return Selection.Viking;
                default:
                    return Selection.Viking;
            }
        }
    }
}