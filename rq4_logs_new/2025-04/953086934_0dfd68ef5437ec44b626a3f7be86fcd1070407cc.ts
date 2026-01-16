import type { ClientData } from "@/types/firebase";

export const CLIENT_FORM_DEFAULT_VALUES: ClientData = {
  first_name: "",
  last_name: "",
  email: "",
  phone: "",
  address: {
    place_id: "",
    primary_text: "",
    secondary_text: "",
    text: "",
  },
};