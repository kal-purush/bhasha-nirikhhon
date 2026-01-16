using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace hm_10
{
    class Figurecs : IСountArea
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Figurecs));

        public const double Pi = 3.14;
        public string FigureName { get; set; }    // Figure name.
        public double EnteredRadius { get; set; } // Entered radius in the figure. Or radius Circle.
        public double SemiPerimeter { get; set; } // Entered SemiPerimeter in the isoscelesTriangle. 
        public double FigureArea { get; set; }

        public virtual double AreaFigure(double EnteredRadius, double SemiPerimeter)
        {
            FigureArea = EnteredRadius * EnteredRadius * Pi;
            return FigureArea;
          
        }

        public virtual  void PrintAreaFigure(double FigureArea)
        {
            Console.WriteLine($"This is: {FigureName}. CLR Type is: {this.GetType()}. Square is: {FigureArea}");
            log.Info("Вывод данных на косноль");
        }
    }
}
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace hm_10
{
    interface IСountArea
    {
        double AreaFigure(double EnteredRadius, double SemiPerimeter);  //  Calculate the area of ​​figure.
        void PrintAreaFigure(double FigureArea);
    }
}