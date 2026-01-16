using System;
using System.Collections.Generic;
using System.Drawing;
using ZedGraph;

namespace Monte_Carlo
{
    partial class Form1
    {
        /// <summary>
        /// Обязательная переменная конструктора.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Освободить все используемые ресурсы.
        /// </summary>
        /// <param name="disposing">истинно, если управляемый ресурс должен быть удален; иначе ложно.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        private ZedGraph.ZedGraphControl zedGraphControl1;

        private List<PointPairList> curveList = new List<PointPairList>();

        private double nVar = 14.0;

        struct Point { 
            public double x; 
            public double y; 

            public Point(double x, double y)
            {
                this.x = x;
                this.y = y;
            }
        }

        double f(double x)
        {
            if (x >= 0 && x < nVar)
                return f1(x);
            else 
                return f2(x);

        }

        double f1(double x)
        {
            return (10 * x) / nVar;
        }

        double f2(double x)
        {
            return 10 * ((x - 20) / (nVar - 20)) + 20;
        }
        
        Point IntersectPoints(Func<double, double> f1, Func<double, double> f2)
        {
            double x = -0.01;
            bool found=false;
            while (!found)
            {
                x+=0.01;
                if (Math.Abs(f1(x)-f2(x)) < 0.001)
                    found = true;
            }
            return new Point(x, f1(x));
        }


        public void Draw()
        {
            PointPairList pointsList1 = new PointPairList();
            PointPairList pointsList2 = new PointPairList();
            GraphPane pane = zedGraphControl1.GraphPane;

            double step = 20.0 / 1000.0;
            double x = 0;
            for(int i=0; i<1000; i++)
            {
                pointsList1.Add(x, f1(x));
                x += step;
            }

            LineItem curve1 = pane.AddCurve("Triangle1", pointsList1, Color.Blue, SymbolType.None);
            step = 20.0 / 1000.0;
            for (int i = 0; i < 1000; i++)
            {
                pointsList2.Add(x, f2(x));
                x += step;
            }

            LineItem curve2 = pane.AddCurve("Triangle2", pointsList2, Color.Green, SymbolType.None);

            zedGraphControl1.AxisChange();
            zedGraphControl1.Invalidate();
            zedGraphControl1.Refresh();
        }
    }
}
