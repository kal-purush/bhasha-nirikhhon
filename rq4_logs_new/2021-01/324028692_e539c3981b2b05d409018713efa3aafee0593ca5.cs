using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using static RandomizerMod.LogHelper;

namespace RandomizerMod.Randomization
{
    public interface IProgressionManager
    {
        bool CanGet(string item);
        void Add(string item);
        void Add(IEnumerable<string> items);
        void AddTemp(string item);
        void Remove(string item);
        void RemoveTempItems();
        void SaveTempItems();
        bool Has(string item);

        void UpdateWaypoints();
        void RecalculateEssence();
        void RecalculateGrubs();

        void AddGrubLocation(string location);
        void AddEssenceLocation(string location, int essence);
    }
}