using System;
using System.Collections.Generic;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;

namespace Lab2.ViewModel
{
    /// <summary>
    /// This class contains properties that the main View can data bind to.
    /// <para>
    /// Use the <strong>mvvminpc</strong> snippet to add bindable properties to this ViewModel.
    /// </para>
    /// <para>
    /// You can also use Blend to data bind with the tool's support.
    /// </para>
    /// <para>
    /// See http://www.galasoft.ch/mvvm
    /// </para>
    /// </summary>
    public class MainViewModel : ViewModelBase
    {
        /// <summary>
        /// Initializes a new instance of the MainViewModel class.
        /// </summary>
        public MainViewModel()
        {
            LemerCommand = new RelayCommand(ExecuteLemerCommand);
            X0 = 7;
            A = 5;
            C = 3;
            N = 16;
            Count = 10000;
        }

        public static event Action<IEnumerable<double>> OnCalculate;

        #region Lemer

        private double _x0;
        public double X0
        {
            get { return _x0; }
            set
            {
                _x0 = value;
                RaisePropertyChanged(() => X0);
            }
        }

        private double _a;
        public double A
        {
            get { return _a; }
            set
            {
                _a = value;
                RaisePropertyChanged(() => A);
            }
        }

        private double _c;
        public double C
        {
            get { return _c; }
            set
            {
                _c = value;
                RaisePropertyChanged(() => C);
            }
        }

        private int _n;
        public int N
        {
            get { return _n; }
            set
            {
                _n = value;
                RaisePropertyChanged(() => N);
            }
        }

        private int _count;
        public int Count
        {
            get { return _count; }
            set
            {
                _count = value;
                RaisePropertyChanged(() => Count);
            }
        }

        private string _result;

        public string Result
        {
            get { return _result; }
            set { _result = value; RaisePropertyChanged(() => Result); }
        }

        #endregion


        public RelayCommand LemerCommand { get; private set; }

        private void ExecuteLemerCommand()
        {
            List<double> x = new List<double>();
            List<double> results = new List<double>();
            var m = Math.Pow(2, N);
            x.Add(X0);

            var start = 0;
            var end = 1;

            double mo = (start + end) / 2.0; // Математическое ожидание
            var D = Math.Pow((start + end), 2) / 12.0; // дисперсия
            var sko = Math.Sqrt(D); // средне квадратическое ожидание

            var r = "М0 = " + mo + "\n";
            r += "D= " + D + "\n";
            r += "SKO =" + sko + "\n";

            Result = r;


            for (var i = 0; i < Count; i++)
            {
                x.Add(Math.Ceiling((A * x[x.Count - 1] + C) % m * 1000.0) / 1000.0);

                var d = Math.Ceiling(GetLimit(x[x.Count - 1] / m) * 1000.0) / 1000.0;
                results.Add(d);
            }

            if (OnCalculate != null)
            {
                OnCalculate.Invoke(results);
            }
        }






        private double GetLimit(double data)
        {
            data = Math.Ceiling((data * 10000) / 10000);
            if (data > 1)
            {
                data = data / 10;
                return GetLimit(data);
            }

            return data;
        }
    }
}