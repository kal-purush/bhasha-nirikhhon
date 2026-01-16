export default (queryParams: URLSearchParams) => {
  if (queryParams.has("redirectTo")) {
    if (redirectIsValid(queryParams)) return `?${queryParams.toString()}`;
    else {
      queryParams.delete("redirectTo");
      return `?${queryParams.toString()}`;
    }
  }
  if (queryParams.size) return `?${queryParams.toString()}`;
  return "";
};

function redirectIsValid(params: URLSearchParams) {
  const redirect = params.get("redirectTo") ?? "";

  if (!redirect.length) {
    return false;
  }
  if (!urlIsValid(redirect)) {
    return false;
  }
  return domainIsAllowed(redirect);
}

function urlIsValid(url: string) {
  const urlRegex =
    /^(https?:\/\/)?([\w-]+\.)*([\w-]+\.[a-z]{2,})(:\d{2,5})?(\/[^\s]*)?(\?.*)?$/;
  return urlRegex.test(url);
}

function domainIsAllowed(url: string) {
  const parsedURL = new URL(url);
  return (
    parsedURL.hostname === "dev.tryber.me" || parsedURL.hostname === "tryber.me"
  );
}