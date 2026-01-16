// This file defines the hosts model used in the entire application

interface HostContent {
  hostname: string;
  ip: string;
  status: boolean;
}

export interface Hosts {
  data: HostContent;
}