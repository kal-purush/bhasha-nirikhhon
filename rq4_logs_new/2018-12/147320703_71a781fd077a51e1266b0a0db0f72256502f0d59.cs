using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BonVoyage
{
    /// <summary>
    /// Informations about resource for a converter
    /// </summary>
    internal class Resource
    {
        internal string Name; // Name of the resource
        internal double Ratio; // Consumption per second
        internal double MaximumAmountAvailable; // Maximum amout of the resource available for usage
        internal double CurrentAmountUsed; // Current amout of the resource used by a converter
    }


    /// <summary>
    /// Class for fuel cells and engines
    /// </summary>
    internal class Converter
    {
        internal bool Use; // Use converter
        internal double OutputValue; // Output value for any output resource (e.g. EC for fuel cells)
        internal List<Resource> InputResources = new List<Resource>();
    }

}