import { api, ResponseT } from "@/utils/api";
import { baseService } from "@/utils/base-service";
import {
  ClientObject,
  DistrictObject,
  Paginate,
  ProvinceObject,
  RegencieObject,
  Token,
  VillageObject,
} from "@/utils/global-types";

type ClientPayload = {
  nama_client: string;
  notelp: string;
  email: string;
  website: string;
  alamat: string;
  kelurahan_id: string;
  kecamatan_id: string;
  kabupaten_id: string;
  provinsi_id: string;
  koordinat1: string;
  koordinat2: string;
};

export default {
  ...baseService,
  async getData(token: Token) {
    return await api.get<ResponseT<Paginate<ClientObject[]>>>(
      "/management/get-clients",
      {
        headers: { Authorization: `Bearer ${token}` },
      }
    );
  },
  async addData(token: Token, data: ClientPayload) {
    return await api.post("/management/create-client", data, {
      headers: { Authorization: `Bearer ${token}` },
    });
  },
  async deleteData(token: Token, id: number) {
    return await api.delete(`/management/delete-client/${id}`, {
      headers: { Authorization: `Bearer ${token}` },
    });
  },
  async getProvinces() {
    return await api.get<ResponseT<ProvinceObject[]>>("/lokasi/provinces");
  },
  async getRegencies(province_id: number) {
    return await api.get<ResponseT<RegencieObject[]>>("/lokasi/regencies", {
      params: { province_id },
    });
  },
  async getDistricts(regency_id: number) {
    return await api.get<ResponseT<DistrictObject[]>>("/lokasi/districts", {
      params: { regency_id },
    });
  },
  async getVillages(district_id: number) {
    return await api.get<ResponseT<VillageObject[]>>("/lokasi/villages", {
      params: { district_id },
    });
  },
  initClientPayload(): ClientPayload {
    return {
      nama_client: "",
      notelp: "",
      email: "",
      website: "",
      alamat: "",
      kelurahan_id: "",
      kecamatan_id: "",
      kabupaten_id: "",
      provinsi_id: "",
      koordinat1: "",
      koordinat2: "",
    };
  },
};

export type { ClientPayload };