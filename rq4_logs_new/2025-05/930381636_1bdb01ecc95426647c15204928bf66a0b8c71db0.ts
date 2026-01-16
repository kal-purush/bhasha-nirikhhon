export type Role =
  | "Administrator"
  | "Dokter"
  | "Perawat"
  | "Rekam Medis"
  | "Apotek"
  | "Analis Laboratorium"
  | "Dinas Kesehatan"
  | "Nutritionist"
  | "Kader"
  | "Kasir"
  | "Dinas Catatan Sipil"
  | "Pendaftaran"
  | "Bidan";

export type RoleObject = {
  id: number;
  name: Role;
  created_at: string | null;
  updated_at: string | null;
};

export type UserObject = {
  id: number;
  name: string;
  username: string;
  role_id: number;
  email: string;
  notelp: string | null;
  nik: string | null;
  nip: string | null;
  sip: string | null;
  kode_bpjs: string | null;
  ihs_id: string | null;
  is_active: boolean;
  cdfix: number;
  created_at: string;
  updated_at: string;
};

export type Module =
  | "Registrasi"
  | "Pasien"
  | "Integrasi-Tools"
  | "Management-Client"
  | "Master-Data"
  | "Layanan";

export type PermissionObject = {
  module: Module;
  can_view: boolean;
  can_edit: boolean;
  can_create: boolean;
  can_delete: boolean;
};

export type ClientObject = {
  id: number;
  is_active: boolean;
  connect_bpjs: boolean;
  nama_client: string;
  notelp: string;
  email: string;
  website: string | null;
  alamat: string | null;
  kelurahan_id: number;
  kecamatan_id: number;
  kabupaten_id: number;
  provinsi_id: number;
  koordinat1: string | null;
  koordinat2: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type AuthObject = {
  user: UserObject;
  client: ClientObject;
  role_permissions: PermissionObject[];
  user_permissions: PermissionObject[];
  role: RoleObject;
};