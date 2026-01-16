using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CoordinateChecker
{
    internal class AxisCube 
    {
        // По-хорошему, нужно, чтобы плоскости были закономерно расположены во всех экземплярах объекта, а то так сложнее работать
        public AxisSquare AxisSquare1 { get; }
        public AxisSquare AxisSquare2 { get; }

        public string SquaresParallelTo { get; }

        public AxisCube(AxisSquare axisPlane1, AxisSquare axisPlane2, string squaresParallelTo)
        {
            AxisSquare1 = axisPlane1;
            AxisSquare2 = axisPlane2;
            SquaresParallelTo = squaresParallelTo;
        }
    }
}