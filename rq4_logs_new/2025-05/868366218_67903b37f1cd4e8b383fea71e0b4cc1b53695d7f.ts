// lib/pinata.ts
import axios from "axios";
import FormData from "form-data";
import fs from "fs";
import path from "path";

const PINATA_API_KEY = process.env.PINATA_API_KEY!;
const PINATA_API_SECRET = process.env.PINATA_API_SECRET!;

export async function uploadImageToIPFS(filePath: string): Promise<string> {
  const url = `https://api.pinata.cloud/pinning/pinFileToIPFS`;

  const data = new FormData();
  const absolutePath = path.resolve(filePath);
  data.append("file", fs.createReadStream(absolutePath));

  const response = await axios.post(url, data, {
    maxBodyLength: Infinity,
    headers: {
      ...data.getHeaders(),
      pinata_api_key: PINATA_API_KEY,
      pinata_secret_api_key: PINATA_API_SECRET,
    },
  });

  return `ipfs://${response.data.IpfsHash}`;
}

export interface CollectionMetadata {
  name: string;
  description: string;
  image: string; // IPFS URI
  external_url?: string;
}

export async function uploadMetadataToIPFS(
  metadata: CollectionMetadata,
): Promise<string> {
  const url = `https://api.pinata.cloud/pinning/pinJSONToIPFS`;

  const response = await axios.post(url, metadata, {
    headers: {
      "Content-Type": "application/json",
      pinata_api_key: PINATA_API_KEY,
      pinata_secret_api_key: PINATA_API_SECRET,
    },
  });

  return `ipfs://${response.data.IpfsHash}`;
}