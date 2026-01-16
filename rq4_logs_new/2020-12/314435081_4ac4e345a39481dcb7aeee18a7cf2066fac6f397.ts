import { Address } from "types/Plan";

export default function formatUserAddress(address: Address) {
  let formattedAddress = "";

  if (address.street) {
    formattedAddress += `${address.street}, `;
  }

  if (address.number) {
    formattedAddress += `${address.number}, `;
  }

  if (address.city) {
    formattedAddress += `${address.city}, `;
  }

  if (address.state) {
    formattedAddress += `${address.state}`;
  }

  if (formattedAddress) {
    formattedAddress += ".";
  }

  return formattedAddress;

  return `${address.street}, ${address.number}, ${address.city}, ${address.state}.`;
}