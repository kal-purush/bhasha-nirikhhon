import { NextRouter } from "next/router";
import basePath from "./basePath";
import getProcessRef from "./getProcessRef";

const prefixUrl = (
  router: NextRouter,
  url: { pathname: string; query?: { [s: string]: string } },
  addProcessRef = true
): { pathname: string; query?: { [s: string]: string } } => {
  let pathname = url.pathname.replace(new RegExp(`^${basePath}`), "");

  if (addProcessRef) {
    const processRef = getProcessRef(router);

    if (processRef && !pathname.startsWith(`/${processRef}`)) {
      pathname = `/${processRef}${pathname}`;
    }
  }

  pathname = `${basePath}${pathname}`;

  return { ...url, pathname };
};

export default prefixUrl;