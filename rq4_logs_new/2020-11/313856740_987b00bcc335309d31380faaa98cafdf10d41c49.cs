using System;
using System.Collections.Generic;

namespace GuessWho
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Guess Who.");
            Characters Liam = new Characters()
            {
                name = "Liam",
                hat = true,
                glasses = true,
                male = true,
                hairColor = "Red",
                facialHair = false,
                eyeColor = "Green",
                earrings = false,
            };
            Characters Noah = new Characters()
            {
                name = "Noah",
                hat = true,
                glasses = false,
                male = true,
                hairColor = "Blond",
                facialHair = true,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Oliver = new Characters()
            {
                name = "Oliver",
                hat = true,
                glasses = true,
                male = true,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Brown",
                earrings = false,
            };
            Characters William = new Characters()
            {
                name = "William",
                hat = true,
                glasses = false,
                male = true,
                hairColor = "Brown",
                facialHair = true,
                eyeColor = "Green",
                earrings = false,
            };
            Characters Elijah = new Characters()
            {
                name = "Elijah",
                hat = true,
                glasses = true,
                male = true,
                hairColor = "Red",
                facialHair = true,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters James = new Characters()
            {
                name = "James",
                hat = true,
                glasses = false,
                male = true,
                hairColor = "Blond",
                facialHair = false,
                eyeColor = "Brown",
                earrings = false,
            };
            Characters Ben = new Characters()
            {
                name = "Ben",
                hat = false,
                glasses = true,
                male = true,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Green",
                earrings = false,
            };
            Characters Lucas = new Characters()
            {
                name = "Lucas",
                hat = false,
                glasses = false,
                male = true,
                hairColor = "Brown",
                facialHair = true,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Mason = new Characters()
            {
                name = "Mason",
                hat = false,
                glasses = true,
                male = true,
                hairColor = "Red",
                facialHair = true,
                eyeColor = "Brown",
                earrings = false,
            };
            Characters Ethan = new Characters()
            {
                name = "Ethan",
                hat = false,
                glasses = false,
                male = true,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Green",
                earrings = false,
            };
            Characters Alexander = new Characters()
            {
                name = "Alexander",
                hat = false,
                glasses = true,
                male = true,
                hairColor = "Blond",
                facialHair = true,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Henry = new Characters()
            {
                name = "Henry",
                hat = false,
                glasses = false,
                male = true,
                hairColor = "Brown",
                facialHair = true,
                eyeColor = "Brown",
                earrings = false,
            };
            Characters Olivia = new Characters()
            {
                name = "Olivia",
                hat = true,
                glasses = false,
                male = false,
                hairColor = "Blond",
                facialHair = false,
                eyeColor = "Green",
                earrings = true,
            };
            Characters Emma = new Characters()
            {
                name = "Emma",
                hat = true,
                glasses = true,
                male = false,
                hairColor = "Red",
                facialHair = false,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Ava = new Characters()
            {
                name = "Ava",
                hat = true,
                glasses = true,
                male = false,
                hairColor = "Brown",
                facialHair = false,
                eyeColor = "Brown",
                earrings = true,
            };
            Characters Sophia = new Characters()
            {
                name = "Sophia",
                hat = true,
                glasses = false,
                male = false,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Green",
                earrings = true,
            };
            Characters Isabella = new Characters()
            {
                name = "Isabella",
                hat = true,
                glasses = true,
                male = false,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Charlotte = new Characters()
            {
                name = "Charlotte",
                hat = true,
                glasses = false,
                male = false,
                hairColor = "Red",
                facialHair = false,
                eyeColor = "Brown",
                earrings = false,
            };
            Characters Amelia = new Characters()
            {
                name = "Amelia",
                hat = false,
                glasses = true,
                male = false,
                hairColor = "Blond",
                facialHair = false,
                eyeColor = "Green",
                earrings = true,
            };
            Characters Mia = new Characters()
            {
                name = "Mia",
                hat = false,
                glasses = true,
                male = false,
                hairColor = "Brown",
                facialHair = false,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Harper = new Characters()
            {
                name = "Harper",
                hat = false,
                glasses = true,
                male = false,
                hairColor = "Brown",
                facialHair = false,
                eyeColor = "brown",
                earrings = true,
            };
            Characters Evelyn = new Characters()
            {
                name = "Evelyn",
                hat = false,
                glasses = false,
                male = false,
                hairColor = "Black",
                facialHair = false,
                eyeColor = "Green",
                earrings = false,
            };
            Characters Abigail = new Characters()
            {
                name = "Abigail",
                hat = false,
                glasses = true,
                male = false,
                hairColor = "Red",
                facialHair = false,
                eyeColor = "Blue",
                earrings = false,
            };
            Characters Emily = new Characters()
            {
                name = "Emily",
                hat = false,
                glasses = false,
                male = false,
                hairColor = "Blond",
                facialHair = false,
                eyeColor = "Brown",
                earrings = true,
            };

            List<Characters> listOfCharacters = new List<Characters>();
            listOfCharacters.Add(Liam);
            listOfCharacters.Add(Noah);
            listOfCharacters.Add(Oliver);
            listOfCharacters.Add(William);
            listOfCharacters.Add(Elijah);
            listOfCharacters.Add(James);
            listOfCharacters.Add(Ben);
            listOfCharacters.Add(Lucas);
            listOfCharacters.Add(Mason);
            listOfCharacters.Add(Ethan);
            listOfCharacters.Add(Alexander);
            listOfCharacters.Add(Henry);
            listOfCharacters.Add(Olivia);
            listOfCharacters.Add(Emma);
            listOfCharacters.Add(Ava);
            listOfCharacters.Add(Sophia);
            listOfCharacters.Add(Isabella);
            listOfCharacters.Add(Charlotte);
            listOfCharacters.Add(Amelia);
            listOfCharacters.Add(Mia);
            listOfCharacters.Add(Harper);
            listOfCharacters.Add(Evelyn);
            listOfCharacters.Add(Abigail);
            listOfCharacters.Add(Emily);
            foreach (var character in listOfCharacters)
            {
                Console.WriteLine($@"
                Name: {character.name}
                Male: {character.male}
                Hat: {character.hat}
                Glasses: {character.glasses}
                Earrings: {character.earrings}
                Facial Hair: {character.facialHair}
                Hair Color: {character.hairColor}
                Eye Color: {character.eyeColor}
                ");
            }
        }
        public class Characters
        {
            public string name;
            public bool hat;
            public bool glasses;
            public bool male;
            public string hairColor;
            public bool facialHair;
            public string eyeColor;
            public bool earrings;
        }
    }
}