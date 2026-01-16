import axios from "axios";

interface HttpMethods {
  get<T>(url: string, params?: string | {}): Promise<T>;
  post<T>(url: string, params?: string | {}): Promise<T>;
  put<T>(url: string, params?: string | {}): Promise<T>;
  delete<T>(url: string, params?: string | {}): Promise<T>;
}

class HttpClient implements HttpMethods {
  private baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl;
  }

  public get<T>(url: string, params?: string | {}): Promise<T> {
    return axios
      .get<T>(this.getUrl(url), {
        headers: this.getHeaders()
      })
      .then(response => {
        return response.data;
      });
  }
  public post<T>(url: string, params?: string | {}): Promise<T> {
    return axios
      .post<T>(this.getUrl(url), params, {
        headers: this.getHeaders()
      })
      .then(response => response.data);
  }
  public put<T>(url: string, params?: string | {} | undefined): Promise<T> {
    return axios
      .put<T>(this.getUrl(url), params, { headers: this.getHeaders() })
      .then(response => response.data);
  }
  public delete<T>(url: string, params?: string | {} | undefined): Promise<T> {
    return axios
      .delete(this.getUrl(url), { headers: this.getHeaders() })
      .then(response => response.data);
  }

  private getUrl(url: string): string {
    return `${this.baseUrl}/${url}`;
  }

  private getHeaders() {
    const token = document.cookie ? document.cookie : null;
    return {
      Authorization: token
    };
  }
}

export default new HttpClient("https://macrop-master.herokuapp.com");