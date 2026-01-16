const fetch = require("node-fetch");
import config from "./config";

// TODO: define type
let announcements: any[] = []; // announcements are stored in memory
let counter = 0;

const fetchQuery = `query getAnnoucements {
  announcements(orderBy: publishedAt_DESC, stage: PUBLISHED) {
    stage
    id
    localizations(locales: [fi, en], includeCurrent: true) {
      description {
        html
      }
      title
      locale
    }
  }
}
`;

const fetchAnnoucements = async () => {
  const apiURI = config.NOTIFICATIONS_URI;
  // only try to fetch announcements if URI is defined in .env
  if (apiURI) {
    counter++;
    console.log(`fetching announcements ${counter} times`);
    const res = await fetch(config.NOTIFICATIONS_URI, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        query: fetchQuery,
        variables: {},
      }),
      cache: "force-cache",
    });
    const data = await res.json();
    announcements = data.data.announcements;
  }
};

fetchAnnoucements();

export { announcements, fetchAnnoucements };