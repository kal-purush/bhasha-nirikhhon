import express, { RequestHandler } from "express";
import crypto from "crypto";
import compression from "compression";
import helmet from "helmet";
import cors from "cors";


import { AuthorizationCode } from "simple-oauth2";

const redirect_uri =
  process.env.REDIRECT_URI ?? "http://localhost:4000/callback";
const scope = process.env.SCOPE ?? "";
const provider = process.env.PROVIDER ?? "github";
const originPattern = process.env.ORIGIN ?? "";

if ("".match(originPattern)) {
  console.warn(
    "Insecure ORIGIN pattern used. This can give unauthorized users access to your repository."
  );
  if (process.env.NODE_ENV === "production") {
    console.error("Will not run without a safe ORIGIN pattern in production.");
    process.exit();
  }
}

const client = new AuthorizationCode({
  client: {
    id: process.env.OAUTH_CLIENT_ID!,
    secret: process.env.OAUTH_CLIENT_SECRET!,
  },
  auth: {
    tokenHost: "https://github.com",
    tokenPath: "/login/oauth/access_token",
    authorizePath: "/login/oauth/authorize",
  },
});

/**
 * Redirects to providers authorization endpoint
 *
 * @param req {Express.RequestRequest}
 * @param res {Express.Response}
 */
export const auth: RequestHandler = (req, res) => {
  const authorizeUri = client.authorizeURL({
    redirect_uri,
    scope,
    state: crypto.randomBytes(16).toString("hex"),
  });

  res.redirect(authorizeUri);
};

/**
 * Fetches token from provider and emulates NetlifyCMS authentication script
 *
 * @param req {Express.Request}
 * @param res {Express.Request}
 * @returns {Express.Response}
 */
export const callback: RequestHandler = async (req, res) => {
  const code = req.query.code;

  if (typeof code !== "string") {
    throw new Error("Invalid code");
  }

  let mess, content;

  try {
    const { token } = await client.getToken({
      code,
      redirect_uri,
      scope,
    });

    mess = "success";
    content = { token: token.access_token, provider };
  } catch (e) {
    mess = "error";
    content = JSON.stringify(e);
  }

  const script = `
      <script nonce="${res.locals.nonce}">
      (function() {
        function receiveMessage(e) {
          console.log("receiveMessage %o", e)
          if (!e.origin.match(${JSON.stringify(originPattern)})) {
            console.log('Invalid origin: %s', e.origin);
            return;
          }
          // send message to main window with da app
          window.opener.postMessage(
            'authorization:${provider}:${mess}:${JSON.stringify(content)}',
            e.origin
          )
        }
        window.addEventListener("message", receiveMessage, false)
        // Start handshake with parent
        console.log("Sending message: %o", "${provider}")
        window.opener.postMessage("authorizing:${provider}", "*")
      })()
      </script>`;
  return res.send(script);
};



const PORT = process.env.PORT ?? 4000;

const app = express();

// Adds a nonce to response for use on inline scripts
app.use((req, res, next) => {
  res.locals.nonce = crypto.randomBytes(16).toString("hex");
  next();
});

app.use(
  helmet({
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        // @ts-expect-error res is of class ServerResponse from http module not express Response. Havent found a way to extend ServerResponse
        "script-src": ["'self'", (req, res) => `'nonce-${res.locals.nonce}'`],
      },
    },
  })
);
app.use(cors());
app.use(
  compression({
    level: 6,
  })
);
app.use(express.json());

const BROWSER_MAX_AGE = 60 * 60;
const CDN_MAX_AGE = 60 * 60 * 24;
const cache: RequestHandler = (req, res, next) => {
  res.set(
    "cache-control",
    `public, max-age=${BROWSER_MAX_AGE}, s-maxage=${CDN_MAX_AGE}`
  );
  next();
};


app.get("/", (_, res) => res.json({ status: "OK 123" }));


// Auth routes for CMS
//app.get("/auth", CMS.auth);
//app.get("/callback", CMS.callback);

app.listen(PORT, () => {
  console.log(`API listening at http://localhost:${PORT}`);
});