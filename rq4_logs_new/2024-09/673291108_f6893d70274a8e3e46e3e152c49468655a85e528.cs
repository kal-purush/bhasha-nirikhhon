using System;
using AwakenServer.Entities;
using Nest;

namespace AwakenServer.StatInfo;

public class StatInfoSnapshot : MultiChainEntity<Guid>
{
    public int StatType { get; set; } // 0 all 1 token 2 pool
    [Keyword] public string Symbol { get; set; }// for StatType= 1
    [Keyword] public string PairAddress { get; set; } // for StatType= 2
    public double Tvl { get; set; }
    public double VolumeInUsd { get; set; }
    public double Price { get; set; }
    public double LpFeeInUsd { get; set; }// for StatType= 2
    public long Period { get; set; }
    public long Timestamp { get; set; }
}