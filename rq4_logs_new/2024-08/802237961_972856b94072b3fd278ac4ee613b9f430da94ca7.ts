export interface createClientI {
  agreement?: {
    active?: boolean;
    antecipation_fee?: string;
    brand?: string | string[];
    charge_type?: string;
    credit_transaction_fee?: string;
    debit_transaction_fee?: string;
    pix_transaction_fee?: string;
    type?: string;
  }[];
  contacts?: any[];
  licenses?: any[];
  master?: {
    cpf?: string;
    email?: string;
    name?: string;
    password?: string;
    phone?: string;
    terminal_password?: string;
    username?: string;
  };
  physical?: {
    address?: string;
    address_number?: string;
    birthdate?: string;
    cep?: string;
    city?: string;
    client_manager?: boolean;
    commercial_name?: string;
    complement?: string;
    cpf?: string;
    district?: string;
    franchise_id?: string;
    module?: string[];
    name?: string;
    phone?: string;
    rg?: string;
    state?: string;
  };
}