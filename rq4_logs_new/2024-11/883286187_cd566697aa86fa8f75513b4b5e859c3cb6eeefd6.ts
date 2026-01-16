import axios from "axios";
import { APP_AUTH } from "./constants/config";

export const API_URL = {
  Property_Count:
    "https://propms.erpnext.syscort.com/api/method/frappe.desk.reportview.get_count?doctype=Property&filters=[[%22is_group%22,%22=%22,1]]",
  Unit_Count:
    "https://propms.erpnext.syscort.com/api/method/frappe.desk.reportview.get_count?doctype=Property&filters=[[%22is_group%22,%22=%22,0]]",
  Tenant_Count:
    "https://propms.erpnext.syscort.com/api/method/frappe.desk.reportview.get_count?doctype=Lease&filters=[[%22Lease%22,%22lease_status%22,%22!=%22,%22Closed%22]]",
  Property_List: "https://propms.erpnext.syscort.com/api/method/property_grid",
  Unit_List:
    "https://propms.erpnext.syscort.com/api/resource/Property?filters=[[%22is_group%22,%22=%22,0]]&fields=[%22name%20as%20unit_number%22,%22custom_location%20as%20location%22,%22parent_property%20as%20property%22,%22unit_owner%22,%22status%22,%22custom_thumbnail_image%20as%20image%22]",
  Tenant_List: "https://propms.erpnext.syscort.com/api/method/tenant_list",
  Owner_List: "https://propms.erpnext.syscort.com/api/method/owner_list",
  Lead_List:
    "https://propms.erpnext.syscort.com/api/resource/Lead?fields=[%22name%22,%22lead_name%22,%22lead_owner%22,%22status%22,%22custom_property%22,%22company%22]",
  Booking_List:
    "https://propms.erpnext.syscort.com/api/resource/Property%20Booking?fields=[%22name%22,%22owner%22,%22creation%22,%22modified%22,%22modified_by%22,%22docstatus%22,%22idx%22,%22property%22,%22book_against%22,%22customer%22,%22annual_rent%22,%22doctype%22]",
  Tenancy_Contract:
    "https://propms.erpnext.syscort.com/api/method/tenancy_contract",
  Create_Property: "https://propms.erpnext.syscort.com/api/resource/Property",
  Create_Tenant: "https://propms.erpnext.syscort.com/api/resource/Customer",
  Create_Owner: "https://propms.erpnext.syscort.com/api/resource/Supplier",
  Create_Lead: "https://propms.erpnext.syscort.com/api/resource/Lead",
  Create_Booking:
    "https://propms.erpnext.syscort.com/api/resource/Property%20Booking",
};

export const loginUser = async (credentials: { usr: string; pwd: string }) => {
  const response = await axios.post(
    "https://propms.erpnext.syscort.com/api/method/login",
    credentials,
    {
      auth: {
        username: APP_AUTH.USERNAME,
        password: APP_AUTH.PASSWORD,
      },
    }
  );
  console.log("response => ", response);
  return response;
};

export const getPropertyCount = async () => {
  const response = await axios.get(API_URL.Property_Count, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getUnitCount = async () => {
  const response = await axios.get(API_URL.Unit_Count, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getTenantCount = async () => {
  const response = await axios.get(API_URL.Tenant_Count, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getPropertyList = async () => {
  const response = await axios.get(API_URL.Property_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getUnitList = async () => {
  const response = await axios.get(API_URL.Unit_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getTenantList = async () => {
  const response = await axios.get(API_URL.Tenant_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getOwnerList = async () => {
  const response = await axios.get(API_URL.Owner_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getLeadList = async () => {
  const response = await axios.get(API_URL.Lead_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getBookingList = async () => {
  const response = await axios.get(API_URL.Booking_List, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const getTenancyContractList = async () => {
  const response = await axios.get(API_URL.Tenancy_Contract, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const createProperty = async (propertyData: any) => {
  const response = await axios.post(API_URL.Create_Property, propertyData, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const fetchProperty = async (params: any) => {
  const response = await axios.get(`${API_URL.Create_Property}/${params}`, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const updateProperty = async (propertyData: any, param: string) => {
  const response = await axios.put(
    `${API_URL.Create_Property}/${param}`,
    propertyData,
    {
      auth: {
        username: APP_AUTH.USERNAME,
        password: APP_AUTH.PASSWORD,
      },
    }
  );
  return response;
};

export const createTenant = async (tenantData: any) => {
  const response = await axios.post(API_URL.Create_Tenant, tenantData, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const createOwner = async (ownerData: any) => {
  const response = await axios.post(API_URL.Create_Owner, ownerData, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const createLead = async (leadData: any) => {
  const response = await axios.post(API_URL.Create_Lead, leadData, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const createBooking = async (bookingData: any) => {
  const response = await axios.post(API_URL.Create_Booking, bookingData, {
    auth: {
      username: APP_AUTH.USERNAME,
      password: APP_AUTH.PASSWORD,
    },
  });
  return response;
};

export const uploadFile = async (file: File) => {
  const formData = new FormData();
  formData.append("file", file);

  try {
    const response = await axios.post(
      "https://propms.erpnext.syscort.com/api/method/upload_file",
      formData,
      {
        auth: {
          username: APP_AUTH.USERNAME,
          password: APP_AUTH.PASSWORD,
        },
        headers: {
          "Content-Type": "multipart/form-data",
        },
      }
    );
    return response;
  } catch (error) {
    console.error("Error uploading file:", error);
  }
};