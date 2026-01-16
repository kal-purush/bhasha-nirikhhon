using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace DataGenerator
{
    internal class Engine
    {
        /// <summary>
        /// datamembers
        /// </summary>
        private SessionParameters SessionParameters { get; set; }
        private SpnLookup Lookup { get; set; }
        private Timer Timer { get; }

        /// <summary>
        /// constructor
        /// </summary>
        public Engine(SpnLookup lookup, SessionParameters p)
        {
            Lookup = lookup;
            SessionParameters = p;
            Timer = new Timer(p.Frequency.TotalMilliseconds);
            Timer.Elapsed += Timer_Elapsed;
        }


        /// <summary>
        /// generates all lines and sends all lines to write output
        /// </summary>
        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {

            Timer.Stop();
            try
            {
                var output = new List<GeneratedLine>();


                for (int i = 0; i < SessionParameters.NumberToSpawn; i++)
                {
                    var vehicleId = new Random().Next(1, SessionParameters.NumberVehicles + 1);
                    var spnNumber = new Random().Next(1, Lookup.SpnCount + 1);
                    var currentSpn = Lookup.Lookup.ToList()[spnNumber].Value;
                    var value = GetValue(currentSpn);
                    var time = DateTime.Now.ToString("MM-dd-yyyy_HH:mm:ss");
                    output.Add(new GeneratedLine(vehicleId, time, currentSpn.SpnNumber, value));
                }
                WriteOutput(output);
            }
            catch (Exception Ee)
            {
                Console.WriteLine(Ee.Message);
            }
            finally
            {
                Timer.Start();
            }
        }

        /// <summary>
        /// creates specified format file name per vehicle
        /// writes all data per vehicle->file
        /// </summary>
        private void WriteOutput(List<GeneratedLine> output)
        {
            foreach (var grup in output.GroupBy(x => x.vehicleId))
            {
                var filename = String.Format("veh{0}_{1}.data", grup.Key, DateTime.Now.ToString("MMddyyyy_hhmmss"));
                StreamWriter FileWriter = new StreamWriter(filename);
                foreach (var i in grup)
                {
                    FileWriter.Write(i.line);
                }

                FileWriter.Close();
            }
        }
        
        /// <summary>
        /// returns valid random value for spn
        /// </summary>
        private string GetValue(Spn spn)
        {
            var rand = new Random();
            double output = 0;
            if (spn.Highvalue <= Int32.MaxValue)
            {
                return HandleSmallValue(spn, rand);
            }
            //determine how likely we are to generate a negative number, and then determine if we should, then do it
            if (spn.Lowvalue < 0)
            {
                return HandleNegativeValue(spn, rand);
            }
            return HandleLargeValue(spn, rand);
        }

        /// <summary>
        /// handles small values < int32 max
        /// returns random within range
        /// </summary>
        private static string HandleSmallValue(Spn spn, Random rand)
        {
            double output = 0;
            int lowval = 0;
            int highval = (int) (spn.Highvalue);
            output = rand.Next(lowval, highval);
            return output.ToString();
        }

        /// <summary>
        /// handles possible negative values
        /// returns random within range
        /// </summary>
        private static string HandleNegativeValue(Spn spn, Random rand)
        {
            double output = 0;
            string s = string.Empty;
            var absLowValue = (double) (Math.Abs(spn.Lowvalue));
            var chanceofnegative = absLowValue + (double) spn.Highvalue;
            chanceofnegative = absLowValue/chanceofnegative;
            int chanceoutof100 = (int) chanceofnegative*100;
            //let fate decide if we should be negative
            //if so:
            if (rand.Next(0, 100) < chanceoutof100)
            {
                //produce negative output value
                if (absLowValue > Int32.MaxValue)
                {
                    return "-" + HandleLargeValue(spn, rand);
                }
                return "-" + HandleSmallValue(spn, rand);
            }
            //otherwise:
            if (spn.Highvalue <= Int32.MaxValue)
            {
                return HandleSmallValue(spn, rand);
            }
            return HandleLargeValue(spn, rand);
        }

        /// <summary>
        /// handles values larger than int32 max
        /// returns random within range
        /// </summary>
        private static string HandleLargeValue(Spn spn, Random rand)
        {
            double output = 0;
            double multiplier = (double) (spn.Highvalue/(decimal) (Int32.MaxValue));
            var num = (int) multiplier;
            var dec = multiplier - num;

            for (int i = 0; i < num; i++)
            {
                output += rand.Next(1, Int32.MaxValue);
            }
            if (dec > 0)
            {
                output += rand.Next(1, (int) (dec*Int32.MaxValue));
            }
            if (spn.Factor > 0)
            {
                var deciml = new Random().Next(0, spn.Factor*10);
                double decPart = deciml/(spn.Factor*10.0);
                output += decPart;
            }
            return output.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// starts timer
        /// </summary>
        public void Start()
        {
            Timer.Start();
        }

        /// <summary>
        /// stops timer
        /// </summary>
        public void Stop()
        {
            Timer.Stop();
        }

        /// <summary>
        /// unused
        /// </summary>
        public void ResetParameters(SessionParameters p)
        {
            SessionParameters = p;
        }

        /// <summary>
        /// unused
        /// </summary>
        public void ResetLookup(SpnLookup lookup)
        {
            Lookup = lookup;
        }
    }
}