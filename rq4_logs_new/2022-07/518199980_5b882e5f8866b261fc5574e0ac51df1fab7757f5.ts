import { getCookie } from "cookies-next";
import { NextApiRequest, NextApiResponse } from "next";
import { AuthenticationClient } from "../../lib/AuthenticationClient";
import { AUTH_CONFIG } from "../../lib/Constants";
import { parseOneNoteResponse } from "../../lib/parsing";
import createPage from "./create-page";

async function foundNotebook(
  client: AuthenticationClient,
  context: any,
  userSelector: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: "onenote/notebooks",
  });
  const response = await clientApi.executeRequest({});
  const value: any[] = response.value || [];

  if (value.length === 0) return false;
  return value.find((notebook: any) => notebook.displayName === "My Profile");
}

async function createNotebook(
  client: AuthenticationClient,
  context: any,
  userSelector: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: "onenote/notebooks",
  });
  const create = await clientApi.executeRequest({
    method: "POST",
    body: JSON.stringify({
      displayName: "My Profile",
    }),
  });

  if (!create.error) {
    return create;
  }

  return false;
}

async function foundSection(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: `onenote/notebooks/${id}/sections`,
  });
  const response = await clientApi.executeRequest({});
  const value: any[] = response.value || [];
  if (value.length === 0) return false;

  return value.find((section: any) => section.displayName === "Training List");
}

async function createSection(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: `onenote/notebooks/${id}/sections`,
  });
  const create = await clientApi.executeRequest({
    method: "POST",
    body: JSON.stringify({
      displayName: "Training List",
    }),
  });

  if (!create.error) {
    return create;
  }

  return false;
}

async function foundPage(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string,
  templateName: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: `onenote/sections/${id}/pages`,
  });
  const response = await clientApi.executeRequest({});
  const value: any[] = response.value || [];
  if (value.length === 0) return false;

  return value.find((page: any) => page.title === templateName);
}

async function createTemplatePage(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string,
  html: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: `onenote/sections/${id}/pages`,
  });
  const create = await clientApi.executeRequest({
    method: "POST",
    contentType: "text/html",
    body: html,
  });

  if (!create.error) {
    return create;
  }

  return false;
}

async function getTemplate(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string,
  templateName: string
) {
  const link = context.req.body.template.contentUrl;
  const onenoteIndex = link.indexOf("onenote");
  const resource = link.substring(onenoteIndex);
  const getUserSelector = link.substring(link.indexOf("users"), onenoteIndex);

  const clientApi = await client.api({
    context,
    userSelector: getUserSelector,
    resource: resource,
  });

  const request = await clientApi.executeRequest({
    method: "GET",
    contentType: "text/html",
    shouldReturnHtml: true,
  });

  if (request.props.htmlContent) return request.props.htmlContent;
}

export async function getStudentPage(
  client: AuthenticationClient,
  context: any,
  userSelector: string,
  id: string
) {
  const clientApi = await client.api({
    context,
    userSelector,
    resource: `onenote/pages/${id}/content?includeIDs=true`,
  });

  const request = await clientApi.executeRequest({
    method: "GET",
    contentType: "text/html",
    shouldReturnHtml: true,
  });

  if (request.props.htmlContent) return request.props.htmlContent;
}

export default async function createStudentProfile(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method === "GET") return res.status(400).json({});
  const response = {};
  const context = { req, res };
  const { student = "", template = "" } = req.body;

  if (!student || !template)
    return res.status(400).json({ error: true, message: "Invalid body" });

  const client = AuthenticationClient.init(AUTH_CONFIG);
  const userSelector = `/users/${student.id}`;

  let getNotebook = await foundNotebook(client, context, userSelector);
  if (!getNotebook)
    getNotebook = await createNotebook(client, context, userSelector);

  let getSection = await foundSection(
    client,
    context,
    userSelector,
    getNotebook.id
  );
  if (!getSection)
    getSection = await createSection(
      client,
      context,
      userSelector,
      getNotebook.id
    );

  let getPage = await foundPage(
    client,
    context,
    userSelector,
    getSection.id,
    template.title
  );

  const getTemplateDoc = await getTemplate(
    client,
    context,
    userSelector,
    getSection.id,
    template.title
  );

  if (getPage) {
    const getStudentPageContent = await getStudentPage(
      client,
      context,
      userSelector,
      getPage.id
    );

    const parsedStudentPage = parseOneNoteResponse(getStudentPageContent);
    const parsedTemplatePage = parseOneNoteResponse(getTemplateDoc);

    if (parsedStudentPage && parsedTemplatePage) {
      const containsAll = parsedTemplatePage.headers.every((header: string) => {
        return parsedStudentPage.headers.indexOf(header) !== -1;
      });

      if (!containsAll) {
        return res.status(200).json({
          updateNecessary: true,
          parsedTemplatePage,
          parsedStudentPage,
          pageId: getPage.id,
        });
      }
    }
  }

  if (!getPage && getTemplateDoc) {
    const createStudentTemplate = await createTemplatePage(
      client,
      context,
      userSelector,
      getSection.id,
      getTemplateDoc
    );

    return res.status(200).json(createStudentTemplate);
  }
  // if (!getPage) {
  //   if (getTemplateDoc) {
  //     const createTemplate = await createTemplatePage(
  //       client,
  //       context,
  //       userSelector,
  //       getSection.id,
  //       getTemplateDoc
  //     );

  //     console.log(createTemplate);
  //   }
  // }

  // console.log(getPage);

  // AURGAUREG

  // const createTrainingList = await createSection(
  //   client,
  //   context,
  //   userSelector,
  //   getNotebook.id
  // );
  // console.log(createTrainingList);

  //const createProfile = await createNotebook(client, context, userSelector);

  //console.log(createProfile);

  res.status(200).json(response);
}