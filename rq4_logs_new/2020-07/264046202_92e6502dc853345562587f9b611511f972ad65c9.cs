using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ComboCounter.Classes
{
    class ClassDefiner
    {
        private static string sex;
        static int weight;

        private static string GetSex()
        {
            return sex;
        }

        private static int GetWeight()
        {
            return weight;
        }

        public static string WeightDivision()
        {
            // Class divisons for Males
            if (GetSex() == "M")
            {
                switch (GetWeight())
                {
                    // Heavyweight
                    case int weight when (weight > 200):
                        return "Heavyweight";


                    // CruiserWeight
                    case int weight when (weight > 175 && weight <= 200):
                        return "Cruiserweight";


                    // Light Heavyweight
                    case int weight when (weight > 168 && weight <= 175):
                        return "Light Heavyweight";


                    // Super Middleweight
                    case int weight when (weight > 160 && weight <= 168):
                        return "Super Middleweight";

                    // Middleweight
                    case int weight when (weight > 154 && weight <= 160):
                        return "Middleweight";

                    // Junior Middleweight
                    case int weight when (weight > 147 && weight <= 154):
                        return "Junior Middleweight";

                    // Welterweight
                    case int weight when (weight > 140 && weight <= 147):
                        return "Welterweight";

                    // Junior Welterweight
                    case int weight when (weight > 135 && weight <= 140):
                        return "Junior Welterweight";

                    // Lightweight
                    case int weight when (weight > 130 && weight <= 135):
                        return "Lightweight";


                    // Junior Lightweight
                    case int weight when (weight > 126 && weight <= 130):
                        return "Junior Lightweight";

                    // Featherweight
                    case int weight when (weight > 122 && weight <= 126):
                        return "Featherweight";

                    // Junior Featherweight
                    case int weight when (weight > 118 && weight <= 122):
                        return "Junior Featherweight";

                    // Bantamweight
                    case int weight when (weight > 115 && weight <= 118):
                        return "Bantamweight";

                    // Junior Bantamweight
                    case int weight when (weight > 112 && weight <= 115):
                        return "Junior Bantamweight";

                    // Flyweight
                    case int weight when (weight > 108 && weight <= 112):
                        return "Flyweight";

                    // Junior Flyweight
                    case int weight when (weight > 105 && weight <= 108):
                        return "Junior Flyweight";

                    // Strawweight
                    case int weight when (weight < 105):
                        return "Strawweight";

                    // Default StrawWeight
                    default:
                        return "Strawweight"; ;
                }
            }
            
            // Class divisions for Females
            else if(GetSex() == "F")
            {
                switch (GetWeight())
                {
                    // Heavyweight
                    case int weight when (weight > 176):
                        return "Heavyweight";   

                    // Light Heavyweight
                    case int weight when (weight > 165 && weight <= 176):
                        return "Light Heavyweight";

                    // Middleweight
                    case int weight when (weight > 154 && weight <= 165):
                        return "Middle Heavyweight";

                    // Junior Middleweight
                    case int weight when (weight > 145 && weight <= 154):
                        return "Junior Middleweight";

                    // Welterweight
                    case int weight when (weight > 138 && weight <= 145):
                        return "Welterweight";

                    // Junior Welterwieght
                    case int weight when (weight > 132 && weight <= 138):
                        return "Junior Welterweight";

                    // Lightwieght
                    case int weight when (weight > 125 && weight <= 132):
                        return "Lightweight";

                    // Featherwieght
                    case int weight when (weight > 119 && weight <= 125):
                        return "Featherweight";

                    // Bantanwieght
                    case int weight when (weight > 114 && weight <= 119):
                        return "Bantamweight";

                    // Junior Bantamwieght
                    case int weight when (weight > 110 && weight <= 114):
                        return "Junior Bantamweight";

                    // Flywieght
                    case int weight when (weight > 106 && weight <= 110):
                        return "Flyweight";

                    // Junior Flywieght
                    case int weight when (weight > 101 && weight <= 106):
                        return "Junior Flyweight";

                    // Pinweight
                    case int weight when (weight < 101):
                        return "Pinweight";

                    default:
                        return "Pinweight";
                   
                }
            }

            else
            {
                return null;
            }
        }

    }
}