using System;
using System.Collections.Generic;

namespace PuzzleSolver.Core.UnitTest.Mockups
{
    internal class BaseMockup
    {
        private Dictionary<string, int> Invocations = new Dictionary<string, int>();

        public void AddOrUpdateInvocation(string key)
        {
            if (Invocations.ContainsKey(key))
                Invocations[key] = Invocations[key]++;
            else
                Invocations.Add(key, 1);
        }

        private int GetInvocationValue(string key)
        {
            if (Invocations.ContainsKey(key))
                return Invocations[key];

            throw new Exception("Key not found!");
        }
    }
}