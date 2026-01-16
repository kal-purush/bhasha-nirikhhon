using MSI.Afterburner;
using OpenHardwareMonitor.Hardware;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace OMineGuard.Backend
{
    public static class Overclocker
    {
        public static event Action ConnectedToMSI;
        public static event Action<List<GPUinfo>> OverclockReceived;

        private static List<OHMgpu> OHMgpus = new List<OHMgpu>();

        private static ControlMemory CM;
        private static int GPUsCount;
        public static bool MSIconnected = false;
        public static DefClock DefaultMSIClock;

        static Overclocker()
        {
            MSIconnecting();
            OHMconnecting();
        }

        private static void MSIconnecting()
        {
            Task.Run(() =>
            {
                while (Process.GetProcessesByName("MSIAfterburner").Length == 0) Task.Delay(100);
                while (!MSIconnected)
                {
                    try
                    {
                        CM = new ControlMemory();
                        MSIconnected = true;
                    }
                    catch { }
                }

                GPUsCount = CM.GpuEntries.Length;
                while (CM.GpuEntries[GPUsCount - 1].PowerLimitMax - 
                       CM.GpuEntries[GPUsCount - 1].PowerLimitMin == 0)
                { GPUsCount = CM.GpuEntries.Length - 1; }

                DefaultMSIClock = new DefClock(GPUsCount);
                for (int i = 0; i < GPUsCount; i++)
                {
                    int core = 0;
                    if (CM.GpuEntries[i].CoreClockBoostMax - CM.GpuEntries[i].CoreClockBoostMin != 0)
                    { core = CM.GpuEntries[i].CoreClockBoostDef / 1000; }
                    else if (CM.GpuEntries[i].CoreClockMax - CM.GpuEntries[i].CoreClockMin != 0)
                    { core = Convert.ToInt32(CM.GpuEntries[i].CoreClockDef) / 1000; }
                    int memory = 0;
                    if (CM.GpuEntries[i].MemoryClockBoostMax - CM.GpuEntries[i].MemoryClockBoostMin != 0)
                    { memory = CM.GpuEntries[i].MemoryClockBoostDef / 1000; }
                    else if (CM.GpuEntries[i].MemoryClockMax - CM.GpuEntries[i].MemoryClockMin != 0)
                    { memory = Convert.ToInt32(CM.GpuEntries[i].MemoryClockDef) / 1000; }

                    DefaultMSIClock.PowerLimits[i] = CM.GpuEntries[i].PowerLimitDef;
                    DefaultMSIClock.CoreClocks[i] = core;
                    DefaultMSIClock.MemoryClocks[i] = memory;
                    DefaultMSIClock.FanSpeeds[i] = CM.GpuEntries[i].FanSpeedDef;
                }
                Task.Run(() => ConnectedToMSI.Invoke());
            });
        }
        private static void OHMconnecting()
        {
            Task.Run(() => 
            {
                Computer c = new Computer { GPUEnabled = true };
                c.Open();
                foreach (IHardware h in c.Hardware)
                {
                    OHMgpus.Add(new OHMgpu(h));
                }
                StartMonitoring();
            });
        }
        private static void StartMonitoring()
        {
            Task.Run(() =>
            {
                List<GPUinfo> GPUinfs;
                List<MSIinfo> listmsi;
                List<OHMinfo> listohm;
                while (true)
                {
                    Task.Run(() => 
                    {
                        GPUinfs = new List<GPUinfo>();
                        // MSIinfo
                        listmsi = new List<MSIinfo>();
                        try
                        {
                            CM.ReloadAll();
                            for (int i = 0; i < GPUsCount; i++)
                            {
                                int core = 0;
                                if (CM.GpuEntries[i].CoreClockBoostMax - CM.GpuEntries[i].CoreClockBoostMin != 0)
                                { core = CM.GpuEntries[i].CoreClockBoostCur / 1000; }
                                else if (CM.GpuEntries[i].CoreClockMax - CM.GpuEntries[i].CoreClockMin != 0)
                                { core = Convert.ToInt32(CM.GpuEntries[i].CoreClockCur) / 1000; }
                                int memory = 0;
                                if (CM.GpuEntries[i].MemoryClockBoostMax - CM.GpuEntries[i].MemoryClockBoostMin != 0)
                                { memory = CM.GpuEntries[i].MemoryClockBoostCur / 1000; }
                                else if (CM.GpuEntries[i].MemoryClockMax - CM.GpuEntries[i].MemoryClockMin != 0)
                                { memory = Convert.ToInt32(CM.GpuEntries[i].MemoryClockCur) / 1000; }

                                listmsi.Add(new MSIinfo
                                {
                                    PowerLimit = CM.GpuEntries[i].PowerLimitCur,
                                    CoreClock = core,
                                    MemoryClock = memory,
                                    FanSpeed = Convert.ToInt32(CM.GpuEntries[i].FanSpeedCur)
                                });
                            }
                        }
                        catch { }
                        //OHMinfo
                        {
                            listohm = new List<OHMinfo>();
                            foreach (OHMgpu ohm in OHMgpus)
                            {
                                ohm.Update();
                                listohm.Add(new OHMinfo
                                {
                                    Temperature = ohm.Temperature,
                                    FanSpeed = ohm.FanSpeed,
                                    CoreClock = ohm.CoreClock,
                                    MemoryClock = ohm.MemoryClock
                                });
                            }
                        }
                        //consolidation
                        for (int i = 0; i < GPUsCount; i++)
                        {
                            if (listmsi.Count == i) listmsi.Add(new MSIinfo());
                            if (listohm.Count == i) listohm.Add(new OHMinfo());
                            GPUinfs.Add(new GPUinfo
                            {
                                MSIinfo = listmsi[i],
                                OHMinfo = listohm[i]
                            });
                        }
                        //event
                        Task.Run(() => OverclockReceived.Invoke(GPUinfs));
                    });
                    Task.Delay(1000);
                }
            });
        }

        private class OHMgpu
        {
            public OHMgpu(IHardware gpu)
            {
                this.gpu = gpu;
                foreach (ISensor s in gpu.Sensors)
                {
                    if (s.SensorType == SensorType.Temperature)
                    {
                        TempSensor = s;
                    }
                    if (s.Name == "GPU Fan" && s.SensorType == SensorType.Control)
                    {
                        TempSensor = s;
                    }
                    if (s.Name == "GPU Core" && s.SensorType == SensorType.Clock)
                    {
                        CoreClockSensor = s;
                    }
                    if (s.Name == "GPU Memory" && s.SensorType == SensorType.Clock)
                    {
                        MemoryClockSensor = s;
                    }
                }
            }

            private IHardware gpu;
            private ISensor TempSensor;
            private ISensor CoreClockSensor;
            private ISensor MemoryClockSensor;
            private ISensor FanSpeedSensor;
            public void Update()
            {
                gpu.Update();
            }

            public int? Temperature
            {
                get
                {
                    if (TempSensor.Value != null)
                    {
                        return Convert.ToInt32(TempSensor.Value.GetValueOrDefault());
                    }
                    else { return null; }
                }
            }
            public int? FanSpeed
            {
                get
                {
                    if (TempSensor.Value != null)
                    {
                        return Convert.ToInt32(FanSpeedSensor.Value.GetValueOrDefault());
                    }
                    else { return null; }
                }
            }
            public int? CoreClock
            {
                get
                {
                    if (CoreClockSensor.Value != null)
                    {
                        return Convert.ToInt32(CoreClockSensor.Value.GetValueOrDefault());
                    }
                    else { return null; }
                }
            }
            public int? MemoryClock
            {
                get
                {
                    if (MemoryClockSensor.Value != null)
                    {
                        return Convert.ToInt32(MemoryClockSensor.Value.GetValueOrDefault());
                    }
                    else { return null; }
                }
            }
        }
    }

    public struct GPUinfo
    {
        public MSIinfo MSIinfo;
        public OHMinfo OHMinfo;
    }
    public struct MSIinfo
    {
        public int? PowerLimit;
        public int? CoreClock;
        public int? MemoryClock;
        public int? FanSpeed;
    }
    public struct OHMinfo
    {
        public int? Temperature;
        public int? FanSpeed;
        public int? CoreClock;
        public int? MemoryClock;
    }
    public class DefClock
    {
        public DefClock(int i)
        {
            PowerLimits = new int[i];
            CoreClocks = new int[i];
            MemoryClocks = new int[i];
            FanSpeeds = new uint[i];
        }

        public int[] PowerLimits;
        public int[] CoreClocks;
        public int[] MemoryClocks;
        public uint[] FanSpeeds;
    }
}