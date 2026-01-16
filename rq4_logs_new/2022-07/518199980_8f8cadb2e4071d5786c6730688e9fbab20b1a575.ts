/**
 * @packageDocumentation
 * @module ApiAuthorize
 */

import { NextApiRequest, NextApiResponse } from "next";

/**
 * @category API Pages
 * @param req
 * @param res
 * @returns
 */
async function ApiAuthorize(req: NextApiRequest, res: NextApiResponse) {
  // Make API request to Azure AD to authenticate current user
  try {
    const graphRequestOptions = {
      method: "POST",
      body: new URLSearchParams({
        client_id: process.env.CLIENT_ID || "",
        scope: "api://33cf93f1-22d5-4c50-8322-61ab9957e8b0/.default",
        client_secret: process.env.CLIENT_SECRET || "",
        grant_type: "client_credentials",
      }),
    };

    const graphRequest = await fetch(
      "https://login.microsoftonline.com/9763eb70-4bf9-4403-84be-2bd40118e7f2/oauth2/v2.0/token",
      graphRequestOptions
    );
    const graphResponse = await graphRequest.json();

    if (graphResponse["access_token"]) {
      return res.status(200).json(graphResponse);
    }

    if (graphResponse["error"]) {
      return res.status(400).json(graphResponse);
    }
  } catch (err) {
    return res.status(400).json(err);
  }

  res.status(200).json({});
}

export default ApiAuthorize;