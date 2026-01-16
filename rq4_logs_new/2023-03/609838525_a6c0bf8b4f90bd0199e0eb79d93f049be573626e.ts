const shrtcoUrl = new URL('https://api.shrtco.de/v2/shorten');

export type FormData = {
  url: string;
};

function createShortLink(params: FormData) {
  shrtcoUrl.search = new URLSearchParams(params).toString();

  return fetch(shrtcoUrl)
    .then((res) => res.json())
    .then(({ ok, result, error }) => {
      if (!ok) throw new Error(error);
      return result;
    });
}

export default createShortLink;