import { ADD_CONTACT } from "types";

export const addContact = (newContact) => ({
  type: ADD_CONTACT,
  newContact: newContact,
});