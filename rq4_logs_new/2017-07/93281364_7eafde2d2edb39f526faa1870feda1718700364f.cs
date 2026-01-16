using System;
using System.Collections.Generic;
using System.Text;

namespace NetSort.UnitTests.EF
{
    public class EF_Score
    {
        [Sortable("value")]
        public int Value { get; set; }
    }
}