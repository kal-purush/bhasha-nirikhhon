export interface Props {
  data: QueriedData;
}
interface QueriedData {
  count: number;
  next: string;
  previous: string | null;
  results: Beneficiary[];
}

export interface Beneficiary {
  id: number;
  first_name: string;
  last_name: string;
  other_name: null;
  gender: string;
  profile_photo: string | null;
  phone_number: string;
  email: string;
  beneficiary_ID: string;
  date_of_birth: Date;
  marital_status: string;
  name_of_spouse: string | null;
  number_of_children: number | null;
  number_of_siblings: number | null;
  education_level: string | null;
  created: Date;
  agent_ID: Agent;
  parent_details: ParentDetails;
}

export interface Agent {
  id: number;
  first_name: string;
  last_name: string;
  birthdate: Date | null;
  agend_ID: string;
  gender: string;
  location: string;
}

export interface ParentDetails {
  id: number;
  father_first_name: string;
  father_last_name: string;
  mother_first_name: string;
  mother_last_name: string;
  address: string;
  father_phone_number: string | null;
  mother_phone_number: string | null;
  father_date_of_birth: Date | null;
  mother_date_of_birth: Date | null;
  father_village: string | null;
  mother_village: string | null;
  created: Date;
}