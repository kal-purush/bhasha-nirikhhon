const getWebsiteText = (website: string) => {
  let link = '';
  let hostname = '';
  try {
    const isUrl = website.startsWith('http');
    const url = new URL(isUrl ? website : `https://${website}`);
    link = url.toString();
    hostname = url.hostname;
  } catch {
    /* no op */
  }
  return { link, hostname };
};

const getGoogleLogoUrl = (url: string) => {
  const { link } = getWebsiteText(url);
  return link ? `https://www.google.com/s2/favicons?domain=${link}&sz=128` : '';
};

export const getLogoUrl = (url: string, logo?: string | null) =>
  getGoogleLogoUrl(logo ?? url);