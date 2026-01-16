import { parse } from "cookie";
import { NextPageContext } from "next";

type RequestedCookie<T> = {
  cookie: T;
  key: string | undefined;
};

function validateCookie(context: RequestedCookie<any>) {
  const { cookie } = context.cookie.req.headers || context;
  const cookies = parse(cookie || "");
  const amountOfCookies = Object.keys(cookies).length;

  if (amountOfCookies === 0) {
    return {
      props: {
        error: {
          code: "NoCookies",
          message: "No cookies were found! Are you in the right spot?",
        },
      },
    };
  }

  if (context["key"]) {
    if (amountOfCookies !== 0 && !cookies[context["key"]]) {
      return {
        props: {
          error: {
            code: "BadResource",
            message: "The selected resource was not found.",
          },
        },
      };
    }

    const parsedCookie = JSON.parse(cookies[context["key"]]);
    return parsedCookie;
  }
}

export default validateCookie;