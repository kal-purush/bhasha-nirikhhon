import { RiskLocation } from "types/Plan";

export default function formatRiskLocation(addressItem: RiskLocation) {
  let formattedAddress = "";

  if (addressItem.identification) {
    formattedAddress += `${addressItem.identification}, `;
  } //
  else {
    return "";
  }

  if (addressItem.street) {
    formattedAddress += `${addressItem.street}, `;
  }

  if (addressItem.neighbor) {
    formattedAddress += `${addressItem.neighbor}, `;
  }

  if (addressItem.complement) {
    formattedAddress += `${addressItem.complement}, `;
  }

  if (addressItem.city) {
    formattedAddress += `${addressItem.city}, `;
  } //
  else {
    return "";
  }

  if (addressItem.state) {
    formattedAddress += `${addressItem.state}`;
  } //
  else {
    return "";
  }

  return formattedAddress;
}