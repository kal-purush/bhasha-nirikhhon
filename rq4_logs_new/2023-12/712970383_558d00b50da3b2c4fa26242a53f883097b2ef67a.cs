using LeetCodeTraining.BuildingH2O;
using System.Text;

namespace LeetCodeTraining.Tests
{
    public class BuildingH2OTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void BuildWaterMoleculesTests(string input, List<string> expectedResults)
        {
            //Act
            string result = BuildingH2OSolution.BuildWaterMolecules(input);

            //Assert
            Assert.Contains(result, expectedResults);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]>()
            {
                new object[]
                {
                    "HOH",
                    new List<string>()
                    {
                        "HHO",
                        "HOH",
                        "OHH"
                    }
                },
                new object[]
                {
                    "OOHHHH",
                    new List<string>()
                    {
                        "HHOHHO",
                        "HOHHHO",
                        "OHHHHO",
                        "HHOHOH",
                        "HOHHOH",
                        "OHHHOH",
                        "HHOOHH",
                        "HOHOHH",
                        "OHHOHH"
                    }
                },
                new object[]
                {
                    "OOHHHHOHH",
                    GeneratePossibleSequences(3)
                },
                new object[]
                {
                    "OHOHHOHHOHHH",
                    GeneratePossibleSequences(4)
                },
                new object[]
                {
                    GeneratePossibleInputs(5),
                    GeneratePossibleSequences(5)
                },
                new object[]
                {
                    GeneratePossibleInputs(6),
                    GeneratePossibleSequences(6)
                }
            };

        private static List<string> GeneratePossibleSequences(int numberOfMolecules)
        {
            if (numberOfMolecules == 0)
            {
                return new List<string>()
                {
                    string.Empty
                };
            }

            List<string> prevGen = GeneratePossibleSequences(numberOfMolecules - 1);
            List<string> output = new List<string>();

            foreach (string seq in prevGen)
            {
                output.Add(seq + "HHO");
                output.Add(seq + "HOH");
                output.Add(seq + "OHH");
            }

            return output;
        }

        private static string GeneratePossibleInputs(int numberOfMolecules)
        {
            int hydrogenNumber = numberOfMolecules * 2;
            int oxygenNumber = numberOfMolecules;
            Random rnd = new Random();
            StringBuilder possibleInput = new StringBuilder();

            while (hydrogenNumber > 0 || oxygenNumber > 0)
            {
                if (rnd.Next(0, 2) == 0 && hydrogenNumber > 0)
                {
                    possibleInput.Append("H");
                    hydrogenNumber--;
                }
                else if (oxygenNumber > 0)
                {
                    possibleInput.Append("O");
                    oxygenNumber--;
                }
            }

            return possibleInput.ToString();
        }
    }
}