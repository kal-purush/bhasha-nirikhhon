using System;
using System.Collections.Generic;

namespace lect14.task2
{
    public class NeighborRepository : INeighbor
    {
        private List<Neighbor> floorNeighbors = new List<Neighbor>();

        public void AddNeighbor(Neighbor neighbor)
        {
            floorNeighbors.Add(neighbor);
        }

        public void AddNeighbor(string fullName, int flatNumber, int phoneNumber)
        {
            Neighbor neighbor = new Neighbor()
            {
                FullName = fullName,
                FlatNumber = flatNumber,
                PhoneNumber = phoneNumber,
            };
            floorNeighbors.Add(neighbor);
        }

        public Neighbor GetNeighbor(int flatNumber)
        {
            if (floorNeighbors.Exists(x => x.FlatNumber.Equals(flatNumber)))
            {
                Neighbor neighbor = floorNeighbors.Find(x => x.FlatNumber.Equals(flatNumber));
                return neighbor;
            }
            else return new Neighbor();
        }
        public void PrintInfoNeighbor(Neighbor neighbor)
        {
            Console.WriteLine($"In this flat lives {neighbor.FullName}. His (her) number phone is " +
                $"{neighbor.PhoneNumber}.");
        }
    }
}