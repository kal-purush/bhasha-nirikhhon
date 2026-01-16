using System;
using System.Collections.Generic;
using System.Linq;
using LiteDB;

namespace PokeRoleBot.Classes
{

    public class Base
    {
        public string Name {get;set;} = ""; //Counts as Nickname for Pokemon
        [BsonRef("Moves")]
        public List<Move> Moves {get;set;} = new List<Move>();
        public Score[] PhyAtributes {get;set;} = new Score[5]; 
        //In order: Strength, Dexterity, Vitality, Special and Insight
        //Note: Trainers do not use Special
        public Score[] MentalAtributes {get;set;} = new Score[5]; 
        //In order: Tough, Cool, Beauty, Cute, Smart/Intellgence
        //Note: For trainers, the 5th value is displayed as Intelligence
        //Note: For Pokemon, the 5th value is displayed as Smart
        public int BaseHP {get;set;} = 3;
        public int HP {get;set;}
        public int MaxWill{get;set;} = 3;
        public int Will {get;set;}
    }
    public class Move
    {
        [BsonId]
        public int Id {get;set;} //Moves will be stored in a seprate collection
        public string Name {get;set;}
        public string Description {get;set;}
        

    }
    public class DexMoves
    {
        [BsonRef("Moves")]
        public Move Move {get;set;}
        public int Cost{get;set;} //Exp cost per move
    }
    public class Score  
    //Blanket class for all Physical, mental, social scores as well as all skills and specialties. 
    //This is to allow the Pokemon and Trainer class to have arrays for said values.
    {
        public int Current {get;set;} = 0;
        public int Max {get;set;} = 5;
    }
    //The following enums are in order to make user input of any field in particular easier
    //Such as allowing the user to input the word "strength" and the service will recieve this as an enum.
    //So we can take this value and reference, for example, PhyAtribute[PhysicalAtributes.Value]
    //Rather than having to parse said string in the the code
    public enum PhysicalAtributes {Strength = 0, Dexterity = 1, Vitality = 2, Special = 3, Insight = 4} 
    public enum SocialAtributes {Tough = 0, Cool = 1, Beauty = 2, Cute = 3, Smart = 4, Intellgence = 4} //Smart/Intelligence returns the same int for obvious reasons

    public enum Target {All, Enemies, Ally, Allies, Area, Self}
}