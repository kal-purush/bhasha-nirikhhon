type EndpointType = {
  url: string;
  method: string;
};

export const ENDPOINTS: Record<string, EndpointType> = {
  products: {
    method: "GET",
    url: "https://fakestoreapi.com/products",
  },
};