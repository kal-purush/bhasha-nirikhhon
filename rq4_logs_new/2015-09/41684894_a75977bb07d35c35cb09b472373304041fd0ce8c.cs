using System;

namespace DealCapture.Client.Dashboards
{
    public sealed class Direction
    {
        public static readonly Direction TraderBuysClientSells = new Direction("Buy", "TraderBuysClientSells");
        public static readonly Direction TraderSellsClientBuys = new Direction("Sell", "TraderSellsClientBuys");
        public static readonly Direction[] Values = {TraderBuysClientSells, TraderSellsClientBuys};

        private readonly string _label;
        private readonly string _dtoValue;

        private Direction(string label, string dtoValue)
        {
            _label = label;
            _dtoValue = dtoValue;
        }

        public string Label { get { return _label; } }

        public string DtoValue { get { return _dtoValue; } }

        public override string ToString()
        {
            return Label;
        }

        public static Direction Parse(string dtoValue)
        {
            switch (dtoValue)
            {
                case "TraderBuysClientSells":
                    return TraderBuysClientSells;
                case "TraderSellsClientBuys":
                    return TraderSellsClientBuys;
            }
            throw new ArgumentOutOfRangeException("dtoValue");
        }
    }
}