import * as express from "express";

const blacklisted_origin: string[] = JSON.parse(
  process.env.BLACKLIST_ORIGIN || "[]"
);

const blacked401UnAuthorized = (origin: string) => {
  return {
    message: `CORS Proxy request from origin "${origin}" is not allowed. Request is unauthorized`,
    issue: "https://github.com/bridgedxyz/base/issues/23",
  };
};

export const blaklistoriginlimit = (
  req: express.Request,
  res: express.Response,
  next
) => {
  const origin = req.headers["origin"];

  if (origin) {
    if (blacked(origin)) {
      res.status(401).send(blacked401UnAuthorized(origin));
      return;
    }
  }
  next();
};

function blacked(origin: string): boolean {
  return blacklisted_origin.includes(origin);
}