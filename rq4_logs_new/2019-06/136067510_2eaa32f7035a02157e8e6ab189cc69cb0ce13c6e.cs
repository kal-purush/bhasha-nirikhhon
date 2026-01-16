using GalaxyZooTouchTable.Log;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace GalaxyZooTouchTable.Lib
{
    public class Log
    {
        public static bool EnableDebugOutput { get; set; }
        public String FileName { get; set; }
        TextWriter log;
        List<String> entryBuffer1;
        List<String> entryBuffer2;
        List<String> activeEntryBuffer;
        List<String> inactiveEntryBuffer;
        int maximumBufferSize = 100;
        object gate;
        bool writingInactiveBufferToFile;
        Thread writerThread;
        bool isOpen;
        DateTime lastCommitted;
        TimeSpan commitInterval = TimeSpan.FromSeconds(10);
        DateTime logStartTime;

        static Log()
        {
            EnableDebugOutput = false; //true
        }

        public Log(String fileNameBase)
        {
            String availableFileName = "";
            bool fileExists = true;
            int fileNameIndex = 0;
            while (fileExists)
            {
                String potentialFileName = fileNameBase + "_" + fileNameIndex + ".csv";
                if (!File.Exists(potentialFileName))
                {
                    availableFileName = potentialFileName;
                    fileExists = false;
                }
                fileNameIndex++;
            }
            FileName = availableFileName;
            //FileName = fileNameBase;
            entryBuffer1 = new List<string>();
            entryBuffer2 = new List<string>();
            activeEntryBuffer = entryBuffer1;
            inactiveEntryBuffer = entryBuffer2;
            gate = new object();
            lastCommitted = DateTime.Now;
            Open();
            logStartTime = DateTime.Now;

            AddHeaders();

            AddEntry("LOG_STARTED");
            writerThread = new Thread(new ThreadStart(WriteLoop)) { IsBackground = true, Priority = ThreadPriority.BelowNormal };
            writerThread.Start();
        }

        public void Open()
        {
            log = new StreamWriter(FileName, true);
            isOpen = true;
        }

        public void Close()
        {
            isOpen = false;
            log.Close();
        }

        public void FinalizeLog()
        {
            AddEntry("LOG_ENDED");
            while (writingInactiveBufferToFile)
            {
                // wait
            }
            WriteActiveBufferToFile();
            while (writingInactiveBufferToFile)
            {
                // wait
            }
            Close();
        }

        public void AddHeaders()
        {
            lock (gate)
            {
                string headers = "time,event,user,context";
                activeEntryBuffer.Add(headers);
                if (EnableDebugOutput) System.Diagnostics.Debug.WriteLine(headers);
            }
            CheckActiveBuffer();
        }

        public void AddEntry(string entry, string user = null, string context = null)
        {
            lock (gate)
            {
                TimeSpan logTime = DateTime.Now - logStartTime;
                //activeEntryBuffer.Add(logTime.Ticks + ";" + entry);
                String entryString = DateTime.Now.Ticks + ";" + entry;
                LogEntry logEntry = new LogEntry(entry, user, context, DateTime.Now.Ticks);
                activeEntryBuffer.Add(logEntry.Print());
                if (EnableDebugOutput) System.Diagnostics.Debug.WriteLine(entryString);
                //DateTime test = new DateTime(DateTime.Now.Ticks);
            }
            CheckActiveBuffer();
        }

        void CheckActiveBuffer()
        {
            if (!writingInactiveBufferToFile && activeEntryBuffer.Count > maximumBufferSize)
            {
                WriteActiveBufferToFile();
            }
        }

        void WriteActiveBufferToFile()
        {
            SwapBuffers();
            WriteInactiveBufferToFile();
        }

        void WriteInactiveBufferToFile()
        {
            writingInactiveBufferToFile = true;
        }

        void SwapBuffers()
        {
            lock (gate)
            {
                List<String> mem = entryBuffer1;
                entryBuffer1 = entryBuffer2;
                entryBuffer2 = mem;
                activeEntryBuffer = entryBuffer1;
                inactiveEntryBuffer = entryBuffer2;
            }
        }

        public void WriteLoop()
        {
            while (true)
            {
                if (writingInactiveBufferToFile && isOpen)
                {
                    foreach (String entry in inactiveEntryBuffer)
                    {
                        log.WriteLine(entry);
                    }
                    inactiveEntryBuffer.Clear();
                    TimeSpan timeSinceLastCommit = DateTime.Now - lastCommitted;
                    if (timeSinceLastCommit > commitInterval)
                    {
                        Close();
                        Open();
                        lastCommitted = DateTime.Now;
                    }
                    writingInactiveBufferToFile = false;
                }
                Thread.Sleep(10000);
            }
        }
    }
}