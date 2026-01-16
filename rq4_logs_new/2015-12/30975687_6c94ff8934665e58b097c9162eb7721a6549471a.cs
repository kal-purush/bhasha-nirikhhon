using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace NetworkNamespace
{
    public class NetworkGraph
    {
        public int[ , ] ArrayOfConnections { get; private set; }

        public List<Computer> ListOfComputers { get; private set;}

        public List<int> NumbersOfInfected { get; private set; }

        public int NumberOfNotInfected { get; set; }

        /// <summary>
        /// Read computers using text data from file "Input.txt"
        /// </summary>
        public NetworkGraph()
        {
            ListOfComputers = new List<Computer>();
            NumbersOfInfected = new List<int>();
            StreamReader sr = new StreamReader("Input.txt");
            string buffer = sr.ReadLine();
            int numberOfComputers = Int32.Parse(buffer);
            NumberOfNotInfected = numberOfComputers;
            ArrayOfConnections = new int[numberOfComputers, numberOfComputers];
            for (int i = 0; i < numberOfComputers; i++)
            {
                for (int j = i; j < numberOfComputers; j++)
                {
                    ArrayOfConnections[i, j] = 0;
                    ArrayOfConnections[j, i] = 0;
                }
            }

            ReadComputersFromFile(sr, numberOfComputers);
            ReadConnections(sr);

            sr.Close();
        }

        /// <summary>
        /// Read computers from file
        /// </summary>
        /// <param name="sr"> stream reader </param>
        /// <param name="numberOfComputers"> total number of computers </param>
        private void ReadComputersFromFile(StreamReader sr, int numberOfComputers)
        {
            for (int i = 0; i < numberOfComputers; i++)
            {
                string buffer = sr.ReadLine();
                string[] splittedBuffer = buffer.Split(' ');
                switch (splittedBuffer[0])
                {
                    case "windows":
                        bool isInfected = Convert.ToBoolean(Convert.ToInt32(splittedBuffer[1]));
                        if (isInfected)
                        {
                            NumberOfNotInfected--;
                            NumbersOfInfected.Add(ListOfComputers.Count);
                        }
                        ListOfComputers.Add(new WindowsComputer(isInfected));
                        break;
                    case "linux":
                        isInfected = Convert.ToBoolean(Convert.ToInt32(splittedBuffer[1]));
                        if (isInfected)
                        {
                            NumberOfNotInfected--;
                            NumbersOfInfected.Add(ListOfComputers.Count);
                        }
                        ListOfComputers.Add(new LinuxComputer(isInfected));
                        break;
                }
            }
        }

        /// <summary>
        /// Get connections between computers from file
        /// </summary>
        /// <param name="sr"> stream reader </param>
        private void ReadConnections(StreamReader sr)
        {
            string buffer;
            while ((buffer = sr.ReadLine()) != null)
            {
                string[] splittedBuffer = buffer.Split(' ');
                int i = Int32.Parse(splittedBuffer[0]);
                int j = Int32.Parse(splittedBuffer[1]);
                ArrayOfConnections[i, j] = 1;
                ArrayOfConnections[j, i] = 1;
            }
        }
    }
}