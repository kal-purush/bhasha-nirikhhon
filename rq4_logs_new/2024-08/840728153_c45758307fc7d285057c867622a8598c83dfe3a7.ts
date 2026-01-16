"use server";
import { revalidateTag } from "next/cache";
import { fetchUrl, fethConfig } from "./fetch-config";
import { BaseFetchArgsType } from "./types";


export const nextFetch = async (args: BaseFetchArgsType) => {
  const { method, url, params, payload, options, revalidate = null } = args;

  const _config = await fethConfig(method, payload, options);
  const _url = await fetchUrl(url, params);
  
  try {
    const response = await fetch(_url, _config);

      if (revalidate) {
        revalidateTag(String(revalidateTag));
      }
      
    return await response.json();
  } catch (error: any) {
    throw new Error(error);
  }
};