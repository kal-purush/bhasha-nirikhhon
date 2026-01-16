using EightCare.Domain.Common;
using System;

namespace EightCare.Domain.KeeperAggregate
{
    public sealed class Molt : Entity
    {
        public DateTime Date { get; private set; }

        public Molt(DateTime date)
        {
            Date = date;
        }
    }
}