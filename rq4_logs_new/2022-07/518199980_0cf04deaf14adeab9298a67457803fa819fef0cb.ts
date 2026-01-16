import DOMPurify from "isomorphic-dompurify";

export const parseOneNoteRequest = (
  headers: any[],
  rows: any[][],
  title?: string
) => {
  const table = document.createElement("table");
  table.setAttribute("data-id", "trainingTable");
  const tableHeaders = document.createElement("thead");
  const tableBody = document.createElement("tbody");
  const headerRow = document.createElement("tr");

  headers.map((header: string) => {
    const headerRowContent = document.createElement("th");
    headerRowContent.innerText = header;
    headerRow.appendChild(headerRowContent);
  });

  tableHeaders.appendChild(headerRow);

  rows.map((row: any[]) => {
    const bodyRow = document.createElement("tr");
    row.map((content: any) => {
      const rowContent = document.createElement("td");
      rowContent.innerText = content;
      bodyRow.appendChild(rowContent);
    });

    tableBody.appendChild(bodyRow);
  });

  table.appendChild(tableHeaders);
  table.appendChild(tableBody);

  const parser = new DOMParser();
  const element = parser.parseFromString(
    `<html><head><title>${title || "Untitled"}</title></head></html>`,
    "text/html"
  );
  element.body.insertAdjacentHTML("beforeend", table.outerHTML);

  return element.documentElement.outerHTML;
};