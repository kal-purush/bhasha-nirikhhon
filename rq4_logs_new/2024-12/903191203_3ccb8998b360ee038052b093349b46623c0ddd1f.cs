using System.Collections.Generic;

namespace FindingCriminal
{
    internal interface ICriminalsFactory
    {
         List<Criminal> Create();
    }
}