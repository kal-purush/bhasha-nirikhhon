import axios from "axios";

interface HttpMethods {
  get<T>(url: string, params?: string | {}): Promise<T>;
  post<T>(url: string, params?: string | {}): Promise<T>;
  put<T>(url: string, params?: string | {}): Promise<T>;
  delete<T>(url: string, params?: string | {}): Promise<T>;
}

class HttpClient implements HttpMethods {
  baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  public get<T>(url: string, params?: string | {}): Promise<T> {
    return axios.get<T>(this.getUrl(url), { params }).then(response => {
      return response.data;
    });
  }
  public post<T>(url: string, params?: string | {}): Promise<T> {
    return axios
      .post<T>(this.getUrl(url), { params })
      .then(response => response.data);
  }
  public put<T>(url: string, params?: string | {} | undefined): Promise<T> {
    return axios
      .put<T>(this.getUrl(url), { params })
      .then(response => response.data);
  }
  public delete<T>(url: string, params?: string | {} | undefined): Promise<T> {
    return axios
      .delete(this.getUrl(url), { params })
      .then(response => response.data);
  }

  private getUrl(url: string): string {
    return `${this.baseUrl}/${url}`;
  }
}

export default new HttpClient("https://macrop-master.herokuapp.com/api");