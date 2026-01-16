using System;
using Trestlebridge.Interfaces;

namespace Trestlebridge.Actions
{
    internal class Goat : IResource, IGrazing
    {
        private Guid _id = Guid.NewGuid();
        private string _shortId {
            get {
                return this._id.ToString().Substring(this._id.ToString().Length - 6);
            }
        }
        public double GrassPerDay { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }

        public string Type { get; } = "Goat";

        public void Graze()
        {
            throw new System.NotImplementedException();
        }
    }
}