const PLAID = require("plaid"),
  { Configuration, PlaidApi, PlaidEnvironments } = PLAID;
require("dotenv").config();

const OAuthClient = require("intuit-oauth"),
  port = 8080,
  allowedOrigins = [
    "https://sausage.saltbank.org",
    "https://i7l8qe.csb.app",
    "https://vau.money",
    "https://jwi5k.csb.app",
    "https://se1dt7.csb.app"
  ], //Origin: <scheme>://<hostname>:<port>
  RESSEND = (res, e) => {
    res.send(e);
    //res.end();
  },
  refererOrigin = (req, res) => {
    var origin = req.query.origin;
    if (!origin) {
      origin = req.headers.origin;
      //"no newaccount made body",  //...printObject(req) //: origin + " " + (storeId ? "storeId" : "")
    }
    return origin;
  },
  allowOriginType = (origin, res) => {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Methods", ["POST", "OPTIONS", "GET"]);
    res.setHeader("Access-Control-Allow-Headers", [
      "Content-Type",
      "Access-Control-Request-Method",
      "Access-Control-Request-Methods",
      "Access-Control-Request-Headers"
    ]);
    //if (res.secure) return null;
    //allowedOrigins[allowedOrigins.indexOf(origin)]
    res.setHeader("Allow", ["POST", "OPTIONS", "GET"]);
    res.setHeader("Content-Type", "Application/JSON");
    var goAhead = true;
    if (!goAhead) return true;
    //if (!res.secure) return true;
    //https://stackoverflow.com/questions/12027187/difference-between-allow-and-access-control-allow-methods-in-http-response-h
  },
  preflight = (req, res) => {
    const origin = req.headers.origin;
    app.use(cors({ origin })); //https://stackoverflow.com/questions/36554375/getting-the-req-origin-in-express
    if (
      [...allowedOrigins, req.body.payingDomains].indexOf(
        req.headers.origin
      ) === -1
    )
      return RESSEND(res, {
        statusCode: 401,
        error: "no access for this origin- " + req.headers.origin
      });
    if (allowOriginType(req.headers.origin, res))
      return RESSEND(res, {
        statusCode,
        statusText: "not a secure origin-referer-to-host protocol"
      });
    //"Cannot setHeader headers after they are sent to the client"

    res.statusCode = 204;
    RESSEND(res); //res.sendStatus(200);
  },
  //const printObject = (o) => Object.keys(o).map((x) => {return {[x]: !o[x] ? {} : o[x].constructor === Object ? printObject(o[x]) : o[x] };});
  standardCatch = (res, e, extra, name) => {
    RESSEND(res, {
      statusCode: 402,
      statusText: "no caught",
      name,
      error: e,
      extra
    });
  },
  timeout = require("connect-timeout"),
  fetch = require("node-fetch"),
  express = require("express"),
  app = express(),
  fill = express.Router(),
  issue = express.Router(),
  attach = express.Router(),
  report = express.Router(),
  disburse = express.Router(),
  database = express.Router(),
  cors = require("cors"),
  stripe = require("stripe")(process.env.STRIPE_SECRET);
//FIREBASEADMIN = FIREBASEADMIN.toSource(); //https://dashboard.stripe.com/account/apikeys

app.use(timeout("5s"));
//catches ctrl+c event
process.on("SIGINT", exitHandler.bind(null, { exit: true }));
// catches "kill pid" (for example: nodemon restart)
process.on("SIGUSR1", exitHandler.bind(null, { exit: true }));
process.on("SIGUSR2", exitHandler.bind(null, { exit: true }));
//https://stackoverflow.com/questions/14031763/doing-a-cleanup-action-just-before-node-js-exits
//http://johnzhang.io/options-req-in-express
//var origin = req.get('origin');

const nonbody = express
  .Router()
  .get("/", (req, res) => res.status(200).send("shove it"))
  .options("/*", preflight);

app.use(express.urlencoded({ extended: false }));
app.use(express.json());

var statusCode = 200,
  statusText = "ok";
//https://support.stripe.com/questions/know-your-customer-(kyc)-requirements-for-connected-accounts

attach
  .post("/quickbooks", async (req, res) => {
    var origin = refererOrigin(req, res);
    if (!req.body || allowOriginType(origin, res))
      return RESSEND(res, {
        statusCode,
        statusText,
        progress: "yet to surname factor digit counts.."
      });
    const oauthClient = new OAuthClient({
      clientId: process.env.QBA_ID,
      clientSecret: process.env.QBA_SECRET,
      environment: "sandbox",
      redirectUri: origin //"https://scopes.cc"
      //logging: true
    });
    if (!oauthClient.authorizeUri)
      return RESSEND(res, {
        statusCode,
        statusText,
        error: "no go oauthClient new"
      });
    var authUri = oauthClient.authorizeUri({
      scope: [OAuthClient.scopes.Accounting],
      state: "intuit-test"
    });
    //res.send(authUri);
    if (!authUri)
      return RESSEND(res, {
        statusCode,
        statusText,
        error: "no go authUri by oauth"
      });
    RESSEND(res, {
      statusCode,
      statusText,
      authUri,
      oauthClient
    });
  })
  .post("/link", async (req, res) => {
    var origin = refererOrigin(req, res);
    if (!req.body || allowOriginType(origin, res))
      return RESSEND(res, {
        statusCode,
        statusText,
        progress: "yet to surname factor digit counts.."
      });

    const configuration = new Configuration({
      basePath: PlaidEnvironments.sandbox,
      baseOptions: {
        headers: {
          "PLAID-CLIENT-ID": process.env.PLAID_CLIENT_ID,
          "PLAID-SECRET": process.env.PLAID_SECRET
        }
      }
    });

    const plaidClient = new PlaidApi(configuration);

    const response = await plaidClient
      .createLinkToken({
        user: {
          client_user_id: process.env.PLAID_CLIENT_ID
        },
        client_name: "QuickNet",
        products: ["auth", "transactions"],
        country_codes: ["US"],
        language: "en",
        webhook: "https://sample-web-hook.com",
        redirect_uri: "https://quick.net.co",
        account_filters: {
          depository: {
            account_subtypes: ["checking", "savings"]
          }
        }
      })
      .catch((e) => {
        standardCatch(res, e, {}, "setup intents (create callback)");
      });

    if (!response.link_token)
      return RESSEND(res, {
        statusCode,
        statusText,
        error: "no go linkToken by plaidClient"
      });
    RESSEND(res, {
      statusCode,
      statusText,
      linkToken: response.link_token
    });
  })
  .post("/plaid", async (req, res) => {
    var origin = refererOrigin(req, res);
    if (!req.body || allowOriginType(origin, res))
      return RESSEND(res, {
        statusCode,
        statusText,
        progress: "yet to surname factor digit counts.."
      });

    const configuration = new Configuration({
      basePath: PlaidEnvironments.sandbox,
      baseOptions: {
        headers: {
          "PLAID-CLIENT-ID": process.env.PLAID_CLIENT_ID,
          "PLAID-SECRET": process.env.PLAID_SECRET
        }
      }
    });

    const plaidClient = new PlaidApi(configuration);

    const response = await plaidClient.itemPublicTokenExchange({
      public_token: req.body.public_token
    });
    const access_token = response.data.access_token;
    const accounts_response = await plaidClient.accountsGet({ access_token });
    const accounts = accounts_response.data.accounts;
    if (!accounts)
      return RESSEND(res, {
        statusCode,
        statusText,
        error: "no go authUri by oauth"
      });
    RESSEND(res, {
      statusCode,
      statusText
    });
  });
//https://stackoverflow.com/questions/31928417/chaining-multiple-pieces-of-middleware-for-specific-route-in-expressjs
app.use(attach); //methods on express.Router() or use a scoped instance
app.listen(port, () => console.log(`localhost:${port}`));
process.stdin.resume(); //so the program will not close instantly
function exitHandler(exited, exitCode) {
  if (exited) {
    console.log("clean");
  }
  if (exitCode || exitCode === 0) console.log(exitCode);
  if (exited.mounted) process.exit(); //bind-only not during declaration
} //bind declare (this,update) when listened on:
process.on("uncaughtException", exitHandler.bind(null, { mounted: true }));
process.on("exit", exitHandler.bind(null, { clean: true }));
function errorHandler(err, req, res, next) {
  console.log("Oops", err);
}
app.use(errorHandler);