import type { Photos } from "photos";

// This interface is shared between PeoplePage and PersonCard
export interface EditablePerson extends Photos.PersonResponse {
  isEditing?: boolean;
  // newName is managed by PersonCard internally
}