import createClient, { Client } from "openapi-fetch";
import type { components, paths } from "./api.d.ts";

export type NewModel = components["schemas"]["NewModel"];
export type NewVendor = components["schemas"]["NewVendor"];

class APIClient {
  base: Client<paths>;

  constructor(base: Client<paths>) {
    this.base = base;
  }

  async createModel(model: NewModel) {
    const { data, error } = await this.base.POST("/models", { body: model });

    if (error !== undefined) {
      throw error;
    }

    return data;
  }

  async getModelByID(id: number) {
    return omitResponse(
      this.base.GET("/models/{modelID}", {
        params: { path: { modelID: id } },
      }),
    );
  }

  async deleteModelByID(id: number) {
    const { error, response } = await this.base.DELETE("/models/{modelID}", {
      params: { path: { modelID: id } },
    });

    if (error === undefined || response.status === 404) {
      return;
    }

    throw error;
  }

  async getModelsByVendor(vendorID: number) {
    return omitResponse(
      this.base.GET("/vendors/{vendorID}/models", {
        params: { path: { vendorID } },
      }),
    );
  }

  async createVendor(vendor: NewVendor) {
    const { data, error } = await this.base.POST("/vendors", { body: vendor });

    if (error !== undefined) {
      throw error;
    }

    return data;
  }

  async getVendors() {
    return omitResponse(this.base.GET("/vendors"));
  }

  async getVendorByID(id: number) {
    return omitResponse(
      this.base.GET("/vendors/{vendorID}", {
        params: { path: { vendorID: id } },
      }),
    );
  }

  async deleteVendorByID(id: number) {
    const { error, response } = await this.base.DELETE("/vendors/{vendorID}", {
      params: { path: { vendorID: id } },
    });

    if (error === undefined || response.status === 404) {
      return;
    }

    throw error;
  }
}

// The server client hits the API directly based on the configured API root.
export const getServerClient = () => {
  const base = createClient<paths>({ baseUrl: process.env.API_BASE_URL });
  return new APIClient(base);
};

// The browser client uses the proxy endpoint.
const browserBase = createClient<paths>({ baseUrl: "/api" });
export const browserClient = new APIClient(browserBase);

/**
 * Omit the raw response from an API call.
 *
 * Since the majority of our API calls return the exact backend response without
 * any manipulation, we only need to omit the `response` property since it is
 * not serializable, and cannot be passed from server to client.
 *
 * @param response - The response from the `openapi-fetch` {@link Client}.
 *
 * @returns - The `data` or `error` from the response. One of those properties is
 * guaranteed to be populated.
 */
async function omitResponse<TData, TError>(
  response: Promise<
    { data: TData; error?: undefined } | { data?: undefined; error: TError }
  >,
) {
  const { data, error } = await response;

  if (error) {
    return { error };
  } else {
    return { data: data! };
  }
}