using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet;

namespace TB_mu2e
{
    class HdmiChannel
    {
        private System.Windows.Forms.Button _button;
        private uint _channel;
        private uint fpga_num;
        private Mu2e_Register _rBias;
        private Mu2e_Register _rLed;
        private Mu2e_Register _rTrim0;
        private Mu2e_Register _rTrim1;
        private Mu2e_Register _rTrim2;
        private Mu2e_Register _rTrim3;

        private bool _isTested = false;

        public System.Windows.Forms.Button button { get { return _button; } set { _button = value; } }
        public uint channel { get { return _channel; } set { _channel = value; } }

        public Mu2e_Register rBias { get { return _rBias; } set { _rBias = value; } }
        public Mu2e_Register rLed { get { return _rLed; } set { _rLed = value; } }
        public Mu2e_Register rTrim0 { get { return _rTrim0; } set { _rTrim0 = value; } }
        public Mu2e_Register rTrim1 { get { return _rTrim1; } set { _rTrim1 = value; } }
        public Mu2e_Register rTrim2 { get { return _rTrim2; } set { _rTrim2 = value; } }
        public Mu2e_Register rTrim3 { get { return _rTrim3; } set { _rTrim3 = value; } }

        public bool isTested { get { return _isTested; } set { _isTested = value; } }

        public HdmiChannel() { }

    }

    class DacProperties
    {
        private Mu2e_Register _register;
        private double _voltage;
        private double[,] voltageData;

        public Mu2e_Register register { get { return _register; } set { _register = value; } }
        public double voltage { get { return _voltage; } set { _voltage = value; } }

        private void FitData(double[] xdata, double[] ydata, out double slope, out double intercept)
        {
            Tuple<double, double> fitParams = MathNet.Numerics.Fit.Line(xdata, ydata);
            slope = fitParams.Item1;
            intercept = fitParams.Item2;
        }
    }
}