interface TicketAuthor {
  _id: string;
  name: string;
  email: string;
  role: string;
  slug: string;
  createdAt: Date;
  updatedAt: Date;
}

interface TicketAssignedTo {
  _id: string;
  name: string;
  email: string;
  role: string;
  slug: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface ITicket {
  _id: string;
  ticketName: string;
  ticketDescription: string;
  ticketType: string;
  ticketStatus: string;
  ticketPriority: string;
  ticketEstimateTimeInHours: number;
  ticketProject: string;
  ticketSlug: string;
  ticketAuthor: TicketAuthor;
  ticketAssignedTo: TicketAssignedTo;
  createdAt: Date;
  updatedAt: Date;
  __v: number;
}