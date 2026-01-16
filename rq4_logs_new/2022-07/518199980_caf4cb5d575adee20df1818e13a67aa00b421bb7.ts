import { setCookie } from "cookies-next";

async function callAPI(fetchOptions: RequestInit) {
  try {
    const createPage = await fetch(
      `${process.env.NEXT_PUBLIC_API_URL}/api/create-page`,
      fetchOptions
    );

    const createPageResponse = await createPage.json();
    const error = createPageResponse.error;

    if (error) console.log(`Error ${JSON.stringify(error)}`);

    const cookieData = {
      tableId: createPageResponse.id,
      headers: createPageResponse.headers,
      rows: createPageResponse.rows,
    };

    setCookie("template", JSON.stringify(cookieData));

    return cookieData;
  } catch (error) {
    console.error(`Error when sending OneNote table: ${error}`);
  }
}

export default async function createTemplate(
  parsed: HTMLElement,
  pageName: string,
  templateName: string,
  tableId: string
) {
  const queryTrainingTable = parsed.querySelector(`[data-id="trainingTable"]`);
  const trainingTable = queryTrainingTable?.outerHTML;
  const htmlOutput = trainingTable || parsed.outerHTML;
  const fetchOptions: any = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: {},
  };

  const shouldUpdateTemplate = pageName === templateName;
  const body = shouldUpdateTemplate
    ? { target: tableId, action: "replace", content: htmlOutput }
    : { html: htmlOutput };

  fetchOptions["body"] = JSON.stringify(body);

  return await callAPI(fetchOptions);
}