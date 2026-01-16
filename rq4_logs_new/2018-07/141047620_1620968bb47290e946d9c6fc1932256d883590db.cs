using System;
using System.Collections.Generic;

namespace Final_TextAdventure
{
    class Creature: GameObject  
    {
        public int Health {get; protected set;}
        public int Damage {get; protected set;}
        public List<Item> Inventory = new List<Item>();

        public void Fight(Player player, NPC enemy)
        {
            bool endFightEarly = false;

            Console.WriteLine("A fight versus " + enemy.Name + " just started.");

            for(;;)
            {
                if(endFightEarly == true)
                    break;

                Console.WriteLine(Environment.NewLine + "You have two Options:");
                Console.WriteLine("Type '1' and press Enter for a normal attack.");
                Console.WriteLine("(You can also type 'exit' and press Enter while fighting to leave the game - but that's no real option.)");

                string input = Console.ReadLine();
                Random rnd = new Random();

                switch (input)
                {
                    case "1":
                        ExecuteHit(player, enemy);
                        break;
                    case "quit": case "q":
                        TextAdventure.IsFinished = true;
                        endFightEarly = true;
                        break;
                    default:
                        Console.WriteLine("Please insert a correct Attack-Code (1 or 2).");
                        break;
                }

                if (enemy.Health == 0)
                {
                    TextAdventure.ConsoleWriteGreen("You won!");
                    enemy.Description += " " + enemy.Name + " also looks pretty dead.";
                    break;
                }

                int enemyAttack = rnd.Next(1, 3);
                
                if(endFightEarly == false)
                    switch (enemyAttack)
                    {
                        case 1:
                            ExecuteHit(enemy, player);
                            break;
                        default:
                            break;
                    }

                if (player.Health == 0)
                {
                    TextAdventure.ConsoleWriteRed("You Lost! Game Over!");
                    TextAdventure.IsFinished = true;
                    break;
                }  
            }
        }

        public void ExecuteHit (Creature attacker, Creature victim)
        {
            Random rnd = new Random();
            int chanceToHit = 80;
            int minDamageMultiplier = 1;
            int maxDamageMultiplier = 2;
            
            
            if (rnd.Next(1, 101) > chanceToHit)
                TextAdventure.ConsoleWriteBlue(attacker.Name + " missed!");
            else 
            {
                var damage = attacker.Damage * rnd.Next(minDamageMultiplier, maxDamageMultiplier+1);
                victim.Health -= damage;
                if (victim.Health <= 0)
                    victim.Health = 0;
                TextAdventure.ConsoleWriteBlue(victim.Name + " took " + damage + " Damage. " + victim.Name + " has " + victim.Health + " Healthpoints left.");
            }
        }

        public void Dialog(Player player, NPC dialogPartner)
        {
            if(!dialogPartner.CanSpeak)
            {
                Console.WriteLine("You can't speak with " + dialogPartner.Name);
                return;
            }

            PlayerDialogModel playerDialog = player.Dialogs.Find(x => x.DialogPartner.Name == dialogPartner.Name);

            Console.WriteLine("A conversation with " + dialogPartner.Name + " just started. Select a answer by typing the number of the line and pressing Enter.");

            for(;;)
            {
                CreatureDialogLine npcLine = dialogPartner.DialogLines.Find(x => x.DialogPhase == playerDialog.DialogPhase);
                TextAdventure.ConsoleWriteBlue(Environment.NewLine + dialogPartner.Name + ": " + npcLine.Line);

                if (npcLine.Gift != null)
                {
                    player.Inventory.Add(npcLine.Gift);
                    dialogPartner.Inventory.Remove(npcLine.Gift);
                }

                foreach(PlayerDialogLine playerDialogLine in playerDialog.DialogLines)
                    if(playerDialogLine.DialogPhase == playerDialog.DialogPhase)
                        Console.WriteLine(playerDialogLine.LineNumber + ": " + playerDialogLine.Line);
                Console.WriteLine("0: (End Conversation)");

                string input = Console.ReadLine();
                int inputNumber;
                if(input == "quit" || input == "q")
                {
                    TextAdventure.IsFinished = true;
                    break;
                }
                else if (input == "0")
                {
                    Console.WriteLine("You ended the conversation.");
                    break;
                }
                else if (Int32.TryParse(input, out inputNumber))
                {
                    PlayerDialogLine playerDialogLine = playerDialog.DialogLines.Find(x => x.LineNumber == inputNumber && x.DialogPhase == playerDialog.DialogPhase);
                    if(playerDialogLine == null)
                        Console.WriteLine("Please insert one of the line numbers from above!");
                    else
                    {
                        if (playerDialogLine.Gift != null)
                        {
                            if(player.Inventory.Contains(playerDialogLine.Gift))
                            {
                                dialogPartner.Inventory.Add(playerDialogLine.Gift);
                                player.Inventory.Remove(playerDialogLine.Gift);
                                TextAdventure.ConsoleWriteBlue("Player: " + playerDialogLine.Line);
                                playerDialog.DialogPhase = playerDialogLine.NewPhase;
                            } 
                            else
                                TextAdventure.ConsoleWriteDarkYellow("You don't have " + playerDialogLine.Gift.Name + " in your inventory.");
                        }
                        else
                        {
                            TextAdventure.ConsoleWriteBlue("Player: " + playerDialogLine.Line);
                            playerDialog.DialogPhase = playerDialogLine.NewPhase;
                        }
                    }
                } 
                else
                    Console.WriteLine("Please insert a valid line Number!");
            }
        }

        #region Helper

        #endregion
    }
}