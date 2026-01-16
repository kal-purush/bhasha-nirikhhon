using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace OMineGuardControlLibrary
{
    public interface IModel : INotifyPropertyChanged
    {
        event Action Autostarted;

        void IniModel();

        IProfile Profile { get; set; }
        List<string> Miners { get; set; }
        IDefClock DefClock { get; set; }
        Dictionary<string, int[]> Algoritms { get; set; }

        string Loggong { get; set; }

        int GPUs { get; set; }
        void ResetGPUs();

        int?[] InfPowerLimits { get; set; }
        int?[] InfCoreClocks { get; set; }
        int?[] InfMemoryClocks { get; set; }
        int?[] InfOHMCoreClocks { get; set; }
        int?[] InfOHMMemoryClocks { get; set; }
        int?[] InfFanSpeeds { get; set; }
        int?[] InfTemperatures { get; set; }
        double?[] InfHashrates { get; set; }
        double? TotalHashrate { get; set; }

        int?[] ShAccepted { get; set; }
        int? ShTotalAccepted { get; set; }
        int?[] ShRejected { get; set; }
        int? ShTotalRejected { get; set; }
        int?[] ShInvalid { get; set; }
        int? ShTotalInvalid { get; set; }

        string WachdogInfo { get; set; }
        string LowHWachdog { get; set; }
        string IdleWachdog { get; set; }
        bool Indicator { get; set; }

        void CMD_SaveProfile(IProfile prof);
        void CMD_RunProfile(IProfile prof, int index);
        void CMD_ApplyClock(IProfile prof, int index);
        void CMD_MinerLogShow();
        void CMD_MinerLogHide();
        void CMD_SwitchProcess();
    }
    public interface IDefClock
    {
        int[] PowerLimits { get; set; }
        int[] CoreClocks { get; set; }
        int[] MemoryClocks { get; set; }
        int[] FanSpeeds { get; set; }
    }
    public interface IProfile
    {
        string RigName { get; set; }
        bool Autostart { get; set; }
        long? StartedID { get; set; }
        string StartedProcess { get; set; }
        int Digits { get; set; }
        List<bool> GPUsSwitch { get; set; }
        List<IConfig> ConfigsList { get; set; }
        List<IOverclock> ClocksList { get; set; }
        int LogTextSize { get; set; }

        bool VkInform { get; set; }
        string VKuserID { get; set; }

        int TimeoutWachdog { get; set; }
        int TimeoutIdle { get; set; }
        int TimeoutLH { get; set; }
    }
    public interface IConfig
    {
        string Name { get; set; }
        string Algoritm { get; set; }
        int? Miner { get; set; }
        string Pool { get; set; }
        string Port { get; set; }
        string Wallet { get; set; }
        string Params { get; set; }
        long? ClockID { get; set; }
        double MinHashrate { get; set; }
        long ID { get; }
    }
    public interface IOverclock
    {
        string Name { get; set; }
        int[] PowLim { get; set; }
        int[] CoreClock { get; set; }
        int[] MemoryClock { get; set; }
        int[] FanSpeed { get; set; }
        long ID { get; }
    }

    internal class Overclock : IOverclock
    {
        public Overclock()
        {
            Name = "Новый разгон";
            ID = DateTime.UtcNow.ToBinary();
        }

        public string Name { get; set; }
        public int[] PowLim { get; set; }
        public int[] CoreClock { get; set; }
        public int[] MemoryClock { get; set; }
        public int[] FanSpeed { get; set; }
        public long ID { get; }
    }
    internal class Config : IConfig
    {
        public Config()
        {
            Name = "Новый конфиг";
            Algoritm = "";
            Pool = "";
            Wallet = "";
            Params = "";
            MinHashrate = 0;
            ID = DateTime.UtcNow.ToBinary();
        }

        public string Name { get; set; }
        public string Algoritm { get; set; }
        public int? Miner { get; set; }
        public string Pool { get; set; }
        public string Port { get; set; }
        public string Wallet { get; set; }
        public string Params { get; set; }
        public long? ClockID { get; set; }
        public double MinHashrate { get; set; }
        public long ID { get; }
    }
}