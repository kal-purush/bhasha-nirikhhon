// src/data/interfaces/Event.ts
export interface Participant {
  _id: string;
  email: string;
  firstName: string;
  lastName: string;
  age: number;
  status: "non-inscrit" | "inscrit";
}

export interface Event {
  _id: string;
  title: string;
  startDate: Date;
  endDate: Date;
  category: "jeune" | "adulte" | "senior" | "toute catégorie";
  status: "ouvert" | "fermé";
  images: string[];
  content: string;
  participants: Participant[];
}

export interface AddEventPopinProps {
  show: boolean;
  onClose: () => void;
  onSave: (formData: FormData) => Promise<void>;
}

export interface EditEventPopinProps {
  show: boolean;
  onClose: () => void;
  onSave: (id: string, formData: FormData) => Promise<void>;
  event: Event;
}

export interface ManageParticipantsPopinProps {
  show: boolean;
  onClose: () => void;
  event: Event;
  onParticipantStatusChange: (eventId: string, participantId: string, status: string) => void;
}