export interface IApiFetch {
  method: string;
  url: string;
  body?: object;
  params?: string | string[];
  file?: boolean;
  formData?: FormData;
}