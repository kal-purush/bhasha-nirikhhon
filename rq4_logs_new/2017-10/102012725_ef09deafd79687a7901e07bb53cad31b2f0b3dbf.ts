export interface Fetcher {

  fetchText(url: string): Promise<string>;
  fetchJson(url: string): Promise<{}>;
  fetchArrayBuffer(url: string): Promise<ArrayBuffer>;

}

export class FetchFetcher implements Fetcher {
  
  fetchText(url: string): Promise<string> {
    return this.fetch(url)
      .then(r => r.text());
  }
  
  fetchJson(url: string): Promise<{}> {
    return this.fetch(url)
      .then(r => r.json());
  }
  
  fetchArrayBuffer(url: string): Promise<ArrayBuffer> {
    return this.fetch(url)
    .then(r => {
      if (r.ok) {
        return this.toArrayBuffer(r)
        .catch(e => Promise.reject(e));
      } else {
        return Promise.reject(r.status + " " + r.statusText + " " + url);
      }
    });
  }
  
  private fetch(url: string): Promise<Response> {
    return fetch(url, { mode:'cors' });
  }

  private toArrayBuffer(response): Promise<any> {
    if (response.arrayBuffer) {
      return response.arrayBuffer();
    } else {
      // isomorphic-fetch does not support response.arrayBuffer
      return new Promise((resolve, reject) => {
        let chunks = [];
        let bytes = 0;

        response.body.on('error', err => {
          reject("invalid audio url");
        });

        response.body.on('data', chunk => {
          chunks.push(chunk);
          bytes += chunk.length;
        });

        response.body.on('end', () => {
          resolve(Buffer.concat(chunks));
        });
      });
    }
  }
  
}