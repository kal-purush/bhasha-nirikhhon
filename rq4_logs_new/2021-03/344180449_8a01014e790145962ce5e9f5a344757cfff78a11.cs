// using System.Collections.Generic;
using System;

namespace ChessWithCohorts.Models
{
    public class Location
    {
        public File File {get; private set;}
        public int Rank {get; private set;}

        public Location(File file, int rank)
        {
            this.File = file;
            this.Rank = rank;
        }

        public bool Equals(Location obj)
        {
            if ((obj == null) || ! this.GetType().Equals(obj.GetType()))
                return false;
            else
            {
                Location l = (Location) obj;
                return (File == l.File) && (Rank == l.Rank);
            }
        }

        public int HashCode()
        {
            return Tuple.Create(File, Rank).GetHashCode();
        }
    }
}