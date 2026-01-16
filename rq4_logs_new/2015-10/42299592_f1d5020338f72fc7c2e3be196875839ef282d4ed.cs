using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GalaSoft.MvvmLight;
using System.Windows;
using GalaSoft.MvvmLight.Ioc;
using GalaSoft.MvvmLight.Command;
using System.Reflection;
using System.Diagnostics;
using System.IO;
using OxyPlot;
using newRBS.Database;

namespace newRBS
{
    /// <summary>
    /// Class that defines a point in an OxyPlot plot.
    /// </summary>
    public class AreaData
    {
        public double x1 { get; set; }
        public double y1 { get; set; }
        public double x2 { get; set; }
        public double y2 { get; set; }
    }

    /// <summary>
    /// Class providing an item for collections, that consists of a <see cref="Name"/> an integer <see cref="Value"/>.
    /// </summary>
    public class NameValueClass : ViewModelBase
    {
        private string _Name;
        public string Name
        {
            get { return _Name; }
            set { _Name = value; RaisePropertyChanged(); }
        }

        private int _Value;
        public int Value
        {
            get { return _Value; }
            set { _Value = value; RaisePropertyChanged(); }
        }

        /// <summary>
        /// Constructor of the class, initializing both, the <see cref="Name"/> and <see cref="Value"/> of the class.
        /// </summary>
        /// <param name="Name">The name of the property.</param>
        /// <param name="Value">The value (int) of the property.</param>
        public NameValueClass(string Name, int Value)
        {
            this.Name = Name;
            this.Value = Value;
        }
    }

    /// <summary>
    /// Class that defines a data point in an OxyPlot with a <see cref="OxyPlot.Axes.DateTimeAxis"/>.
    /// </summary>
    public class TimeSeriesEvent
    {
        public DateTime Time { get; set; }
        public double Value { get; set; }
    }

    /// <summary>
    /// Class that writes logging traces to the console window.
    /// </summary>
    class MyTextWriterTraceListener : TextWriterTraceListener
    {
        public MyTextWriterTraceListener(string fileName) : base(fileName)
        {
        }

        public override void Write(string message)
        {
            base.Write(String.Format("[{0}]:{1}", DateTime.Now, message));
        }

        public override void TraceEvent(TraceEventCache eventCache, string source, TraceEventType eventType, int id, string message)
        {
            if (string.IsNullOrEmpty(message)) return;

            WriteLine(string.Format("{0}, {1}, {2}, {3}", DateTime.Now.ToString("[yyyy-MM-dd HH:mm:ss.fff]"), eventType, source, message.Replace("\r", string.Empty).Replace("\n", string.Empty)));
        }
    }

    /// <summary>
    /// Class that defines the logging of the program. There is one listener for the console window and one for the log file.
    /// </summary>
    public static class TraceSources
    {
        private static MyTextWriterTraceListener textWriterTraceListener = new MyTextWriterTraceListener("Logs/LogStart_" + DateTime.Now.ToString("yyyy-MM-dd") + ".log");

        public static TraceSource Create(string sourceName)
        {
            var source = new TraceSource(sourceName);

            // Console listemer
            Essential.Diagnostics.ColoredConsoleTraceListener listener1 = new Essential.Diagnostics.ColoredConsoleTraceListener();
            listener1.Template = "{DateTime:'['HH':'mm':'ss'.'fff']'}, {EventType}, " + sourceName + ", {Message}{Data}";
            listener1.ConvertWriteToEvent = true;
            listener1.Filter = new EventTypeFilter(SourceLevels.All);
            source.Listeners.Add(listener1);

            // Log file listener
            string path = "Logs/";
            DirectoryInfo di = Directory.CreateDirectory(path);
            MyTextWriterTraceListener listener2 = textWriterTraceListener;
            listener2.Filter = new EventTypeFilter(SourceLevels.Information);
            Trace.AutoFlush = true;
            source.Listeners.Add(listener2);

            source.Switch.Level = SourceLevels.All;
            return source;
        }
    }

    /// <summary>
    /// Class that stores the chopper configuration
    /// </summary>
    public class ChopperConfig : ViewModelBase
    {
        private int _LeftIntervalChannel;
        public int LeftIntervalChannel { get { return _LeftIntervalChannel; } set { _LeftIntervalChannel = value; RaisePropertyChanged(); } }

        private int _RightIntervalChannel;
        public int RightIntervalChannel { get { return _RightIntervalChannel; } set { _RightIntervalChannel = value; RaisePropertyChanged(); } }

        private int _IonMassNumber;
        public int IonMassNumber { get { return _IonMassNumber; } set { _IonMassNumber = value; RaisePropertyChanged(); } }

        private double _IonEnergy;
        public double IonEnergy { get { return _IonEnergy; } set { _IonEnergy = value; RaisePropertyChanged(); } }
    }



    /// <summary>
    /// Class that contains user data
    /// </summary>
    public class MyUser : ViewModelBase
    {
        public string UserName { get; set; }
        public string LoginName { get; set; }
        public string Database { get; set; }
    }
}