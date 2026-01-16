import { getCookie, setCookie } from "cookies-next";

type OneNoteRequestOptions = {
  [key: string]: any;
  headers?: any[];
  rows?: any[];
};

class OneNoteRequest {
  config: OneNoteRequestOptions = {};

  constructor(config: OneNoteRequestOptions) {
    this.config = config;
  }

  private parseHeaders(headers: any[]) {
    const table = document.createElement("table");
    const headerSection = document.createElement("thead");
    const headerRow = document.createElement("tr");
    headers.map((header: any) => {
      const headerElement = document.createElement("th");
      const headerContent = document.createTextNode(header);
      headerElement.appendChild(headerContent);
      headerRow.appendChild(headerElement);
    });

    headerSection.appendChild(headerRow);
    table.appendChild(headerSection);

    return table;
  }

  private parseRows(rows: any[], table: HTMLElement) {
    const bodySection = document.createElement("tbody");
    const bodyRow = document.createElement("tr");
    rows.map((row: any) => {
      const rowElement = document.createElement("th");
      const rowContent = document.createTextNode(row);
      rowElement.appendChild(rowContent);
      bodyRow.appendChild(rowElement);
    });

    bodySection.appendChild(bodyRow);
    table.appendChild(bodySection);

    return table;
  }

  parseTableToSend() {
    if (typeof document === "undefined") return false;

    const headers = this.config.headers || [];
    const rows = this.config.rows || [];

    if (headers.length === 0 || rows.length === 0) return false;
    const table = this.parseHeaders(headers);
    const finalTable = this.parseRows(rows, table);
    const finalTableAsString = finalTable.outerHTML;

    return finalTableAsString;
  }
}

export type { OneNoteRequestOptions };
export { OneNoteRequest };