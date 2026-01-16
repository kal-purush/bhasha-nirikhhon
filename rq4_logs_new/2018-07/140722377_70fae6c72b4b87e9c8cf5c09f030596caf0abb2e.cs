using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cars2._0.Models
{
    class CarEntityEqualityComparer : IEqualityComparer<object>
    {
        public new bool Equals(object x, object y)
        {
            if (x.GetType() != y.GetType())
                return false;
            else{
                CarEntity xc = (CarEntity)x;
                CarEntity yc = (CarEntity)y;
                if (xc.tradeMark == yc.tradeMark &&
                    xc.model == yc.model &&
                    xc.gearBox == yc.gearBox &&
                    xc.fuel == yc.fuel &&
                    xc.Engine == yc.Engine &&
                    xc.MileAge == yc.MileAge &&
                    xc.HorsePower == yc.HorsePower &&
                    xc.Title.Equals(yc.Title))
                    return true;
            }
            return false;
        }

        public int GetHashCode(object obj)
        {
            return base.GetHashCode() +
                new Random().Next(int.MinValue, int.MinValue) +
                new Random().Next(int.MinValue, int.MinValue) +
                new Random().Next(int.MinValue, int.MinValue) +
                new Random().Next(int.MinValue, int.MinValue) +
                new Random().Next(int.MinValue, int.MinValue);
        }
    }
}