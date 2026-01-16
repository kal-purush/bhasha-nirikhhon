import fetch from "node-fetch";
import { JSONRPCError } from "@open-rpc/server-js";

const apikey: string = "" || process.env.SHIPPING;
const carrierID: string = "" || process.env.SHIPPINGCARRIERID;

export const getShippingRates: GetShippingRates = async (userFirst: string, userLast: string, userStreet: string, userStreet2: string, userCity: string, userState: string, userZipcode: string, userCountry: string, userPhone: string, weightUnits: string, weightAmount: string, userCompany: string ) => {
  const body: any = {
    rate_options: {
      carrier_ids: [
        carrierID,
      ],
    },
    shipment: {
      validate_address: "no_validation",
      ship_to: {
        company_name: userCompany,
        name: userFirst + " " + userLast,
        phone: userPhone,
        address_line1: userStreet,
        address_line2: userStreet2,
        city_locality: userCity,
        state_province: userState,
        postal_code: userZipcode,
        country_code: userCountry,
        address_residential_indicator: "yes",
      },
      ship_from: {
        company_name: "",
        name: "",
        phone: "",
        address_line1: "",
        address_line2: "",
        city_locality: "",
        state_province: "",
        postal_code: "",
        country_code: "",
        address_residential_indicator: "yes",
      },
      packages: [
        {
          weight: {
            value: weightAmount,
            unit: weightUnits,
          },
        },
      ],
    },
  };
  const response: any = await fetch("https://api.shipengine.com/v1/rates", {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Host": "api.shipengine.com",
      "API-Key": apikey,
      "Content-Type": "application/json",
    },
  });
  const data = await response.json();
  console.log(data);
  return data;
};

export const getShippingLabel: GetShippingLabel = async ( shipingRateID: string )  => {
  const body: any = {
    label_format: "pdf",
    label_layout: "4x6",
  };
  const response: any = await fetch("https://api.shipengine.com/v1/labels/rates/" + shipingRateID, {
    method: "POST",
    body: JSON.stringify(body),
    headers: {
      "Host": "api.shipengine.com",
      "API-Key": apikey,
      "Content-Type": "application/json",
    },
  });
  const data = await response.json();
  console.log(data);
  if (data.errors) {
    return data;
  } else {
    return data;
  }
};
export const trackUsingLabelID: TrackUsingLabelID = async ( labelID: string ) => {
  console.log("Getting Label");

  const response: any = await fetch("https://api.shipengine.com/v1/labels/" + labelID + "/track", {
    method: "GET",
    headers: {
      "Host": "api.shipengine.com",
      "API-Key": apikey,
      "Content-Type": "application/json",
    },
  });
  const data = await response.json();
  console.log(data);
  return data;
};