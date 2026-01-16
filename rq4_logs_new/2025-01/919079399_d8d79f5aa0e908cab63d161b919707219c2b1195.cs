using Models.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Comparers
{
    public class FuturesAccountTradeComparer : IEqualityComparer<FuturesAccountTradeResponseDto>
    {
        private const int Precision = 8;

        public bool Equals(FuturesAccountTradeResponseDto x, FuturesAccountTradeResponseDto y)
        {
            if (x == null || y == null)
                return false;

            return x.Symbol == y.Symbol &&
                   x.Id == y.Id &&
                   x.OrderId == y.OrderId &&
                   x.Side == y.Side &&
                   x.PositionSide == y.PositionSide;
        }

        public int GetHashCode(FuturesAccountTradeResponseDto obj)
        {
            if (obj == null)
                return 0;

            return HashCode.Combine(
                                        obj.Symbol,
                                        obj.Id,
                                        obj.OrderId,
                                        obj.Side,
                                        obj.PositionSide
                                    );
        }
    }
}
