using System;
using System.Collections.Generic;
using System.Text;

namespace MedRoad.Ant
{
    public class AntStats
    {
        private Dictionary<byte, int> pageCounts = new Dictionary<byte, int>();
        private int rxFailCount = 0;
        private int totalCount = 0;

        public void IncrementPageCount(byte pageNumber)
        {
            int count;
            pageCounts.TryGetValue(pageNumber, out count);
            pageCounts[pageNumber] = ++count;

            totalCount++;
        }

        public void IncrementRxFailCount()
        {
            rxFailCount++;
            totalCount++;
        }

        public override string ToString()
        {
            if (totalCount == 0)
                return String.Empty;

            StringBuilder b = new StringBuilder();
            b.AppendFormat("RX_FAIL {0,6:d}  {1,5:f1}%\n", rxFailCount, 100f * rxFailCount / totalCount);
            foreach (KeyValuePair<byte, int> kvp in pageCounts)
                b.AppendFormat("0x{0:X2}    {1,6:d}  {2,5:f1}%\n", kvp.Key, kvp.Value, 100f * kvp.Value / totalCount);
            b.AppendFormat("TOTAL   {0,6:d}        \n", totalCount);
            return b.ToString();
        }

    }
}