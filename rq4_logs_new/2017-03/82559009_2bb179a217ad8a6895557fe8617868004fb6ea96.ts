// This file defines the metric model used in the entire application

interface CpuCore {
  frequency: string;
  name: string;
  usage: string;
}

interface Process {
  CPUOccupied: string;
  GUID: string;
  PID: string;
  RAMOccupied: string;
  UID: string;
  name: string;
  status: boolean;
}

interface Partition {
  filesystem: string;
  mountPt: string;
  name: string;
  storage: string;
}

export interface Metrics {
  hostname: string;
  status: boolean;
  process: boolean;
  ram: string;
  storage: string;
  cpuData?: { cores: CpuCore[] };
  processData?: { processes: Process[] }; 
  storageData?: { storagePartitions: Partition[] };
  ramData?: { buffer: string, swapUsage: string, totalMemory: string };
}