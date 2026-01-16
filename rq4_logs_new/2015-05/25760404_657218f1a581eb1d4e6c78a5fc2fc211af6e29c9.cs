using System;
using System.ComponentModel;

namespace SeaBattle.Common.Utils
{
    public class GarbageCollector
    {
        private readonly BackgroundWorker _backgroundWorker = new BackgroundWorker();

        private int _stepNumber = 0;
        private int _sleepStep = 1;

        public int SleepStep
        {
            get
            {
                return _sleepStep;
            }
            set
            {
                _sleepStep = value;
            }
        }

        public GarbageCollector()
        {
            Init();
        }

        public GarbageCollector(int sleepStep)
        {
            _sleepStep = sleepStep;
            Init();
        }

        private void Init()
        {
            _backgroundWorker.DoWork += BackgroundWorkerDoWork;
        }

        public void Update()
        {
            if (_backgroundWorker.IsBusy) return;
            if (++_stepNumber < _sleepStep) return;
            _stepNumber = 0;
            _backgroundWorker.RunWorkerAsync();
        }

        private static void BackgroundWorkerDoWork(Object sender, EventArgs e)
        {
            GC.Collect(1);
        }
    }
}