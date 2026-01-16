using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace bitsAndPieces
{
    class Analysis
    {
        public int turnaroundTime(int arrival, int endTime)
        {
            int turnaroundTime = endTime - arrival;
            return turnaroundTime;
        }

        public int ResponseTime(int arrival, int startTime)
        {
            int responseTime = startTime - arrival;
            return responseTime;
        }
        
        public void OutputAnalysisOneProcess(Metadata log)
        {
            turnaround = turnaroundTime(log.get(submitted), log.get(completed));
            response = ResponseTime(log.get(submitted), log.get(start));

        }

        public double AverageStartTime(List<Metadata> logList)
        {
            double average = 0;
            for (int i = 0; i < logList.Count; i++)
            {
                average += System.Convert.ToDouble(logList[i].get(start));
            }
            average /= logList.Count;
            return average;
        }

        public double AverageEndTime(List<Metadata> logList)
        {
            double average = 0;
            for (int i = 0; i < logList.Count; i++)
            {
                average += System.Convert.ToDouble(logList[i].get(completed));
            }
            average /= logList.Count;
            return average;
        }

        public double AverageTurnaroundTime(List<Metadata> logList)
        {
            double average = 0;
            for (int i = 0; i < logList.Count; i++)
            {
                double turn = System.Convert.ToDouble(turnaroundTime(logList[i].get(submitted), logList[i].get(start)));
                average += turn;
            }
            average /= logList.Count;
            return average;
        }

        public double AverageResponseTime(List<Metadata> logList)
        {
            double average = 0;
            for (int i = 0; i < logList.Count; i++)
            {
                double turn = System.Convert.ToDouble(ResponseTime(logList[i].get(submitted), logList[i].get(completed)));
                average += turn;
            }
            average /= logList.Count;
            return average;
        }

        public void DisplayAverages(Dictionary<int, Metadata> logInfo)
        {
            List<Metadata> logList = new List<Metadata>();
            logList.Clear();
            for(int i = 0; i < logInfo.Count; i++)
            {
                logList.Add(logInfo[i].second);
            }
            double responseAvg = AverageResponseTime(logList);
            double turnAroundAvg = AverageTurnaroundTime(logList);
            double startAvg = AverageStartTime(logList);
            double endAvg = AverageEndTime(logList);

            Console.WriteLine("Averages\n");
            Console.WriteLine("Response Time: {0}, turnaround Time: {1}, Start Time: {2}, end Time: {3}", 
                responseAvg, turnAroundAvg, startAvg, endAvg);
        }
    }
}