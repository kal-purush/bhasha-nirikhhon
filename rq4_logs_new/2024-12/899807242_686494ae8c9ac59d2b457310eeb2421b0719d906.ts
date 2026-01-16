export interface TicketMetadata {
  statuses: {
    id: number;
    name: string;
    lucideIcon: string;
    hexColor: string;
  }[];
  types: { id: number; name: string; lucideIcon: string; hexColor: string }[];
  priorities: {
    id: number;
    name: string;
    lucideIcon: string;
    hexColor: string;
  }[];
}

export interface TicketStatus {
  id: number;
  name: string;
  lucideIcon: string;
  hexColor: string;
}

export interface TicketType {
  id: number;
  name: string;
  lucideIcon: string;
  hexColor: string;
}

export interface TicketPriority {
  id: number;
  name: string;
  lucideIcon: string;
  hexColor: string;
}