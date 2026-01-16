import { z } from "zod";
import ParamsI from "../../__interfaces/queryParams.interface";

const FranchiseSchema = z.object({
  franchise_name: z.string().optional().nullable(),
  id: z.string().optional().nullable(),
});
const MasterSchema = z.object({
  document: z.string().optional().nullable(),
  email: z.string().optional().nullable(),
  name: z.string().optional().nullable(),
  phone: z.string().optional().nullable(),
  username: z.string().optional().nullable(),
});
const PromoterAddress = z.object({
  active: z.boolean().optional().nullable(),
  address: z.string().optional().nullable(),
  cep: z.string().optional().nullable(),
  city: z.string().optional().nullable(),
  complement: z.string().optional().nullable(),
  created_at: z.string().optional().nullable(),
  district: z.string().optional().nullable(),
  id: z.string().optional().nullable(),
  number: z.string().optional().nullable(),
  promoter_id: z.string().optional().nullable(),
  state: z.string().optional().nullable(),
  updated_at: z.string().optional().nullable(),
});
const PromoterAgreement = z.object({
  active: z.boolean().optional().nullable(),
  antecipation_fee: z.string().optional().nullable(),
  brand: z.any(),
  charge_type: z.string().optional().nullable(),
  credit_transaction_fee: z.string().optional().nullable(),
  debit_transaction_fee: z.string().optional().nullable(),
  emission_fee: z.string().optional().nullable(),
  id: z.string().optional().nullable(),
  pix_transaction_fee: z.string().optional().nullable(),
  type: z.string().optional().nullable(),
});
const PromoterJuridic = z.any();
const PromoterLicenses = z.object({
  active: z.boolean().optional().nullable(),
  id: z.string().optional().nullable(),
  type: z.string().optional().nullable(),
  value: z.string().optional().nullable(),
});
const PromoterPOSModule = z.object({
  POSModule: z.object({
    active: z.boolean().optional().nullable(),
    description: z.string().optional().nullable(),
    id: z.string().optional().nullable(),
    name: z.string().optional().nullable(),
  }),
  id: z.string().nullable().optional(),
});
const PromoterPerson = z.object({
  active: z.boolean().optional().nullable(),
  birthdate: z.string().optional().nullable(),
  commercial_name: z.string().optional().nullable(),
  cpf: z.string().optional().nullable(),
  created_at: z.string().optional().nullable(),
  id: z.string().optional().nullable(),
  name: z.string().optional().nullable(),
  promoter_id: z.string().optional().nullable(),
  rg: z.string().optional().nullable(),
  updated_at: z.string().optional().nullable(),
});

const PromoterByIdSchema = z.object({
  Contacts: z.any(),
  Franchise: FranchiseSchema.optional().nullable(),
  Master: MasterSchema.optional().nullable(),
  PromoterAddress: PromoterAddress.optional().nullable(),
  PromoterAgreement: z.array(PromoterAgreement).optional().nullable(),
  PromoterJuridic: PromoterJuridic.optional().nullable(),
  PromoterLicenses: z.array(PromoterLicenses).optional().nullable(),
  PromoterPOSModule: z.array(PromoterPOSModule).optional().nullable(),
  PromoterPerson: PromoterPerson.optional().nullable(),
  active: z.boolean().optional().nullable(),
  created_at: z.string().optional().nullable(),
  franchise_id: z.string().optional().nullable(),
  id: z.string().optional().nullable(),
  master_id: z.string().optional().nullable(),
  promoter_name: z.string().optional().nullable(),
  ref_id: z.string().optional().nullable(),
  updated_at: z.string().optional().nullable(),
  use_products_database: z.boolean().optional().nullable(),
  client_manager: z.boolean().optional().nullable(),
});

export type PromoterById = z.infer<typeof PromoterByIdSchema>;

export interface PromoterByIdsParams extends ParamsI {
  s?: string;
  f?: string[];
  w?: any;
}