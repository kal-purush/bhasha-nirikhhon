export function downloadStaticResource(src) {
  const resourceUrl = src;
  const downloadLink = document.createElement("a");
  downloadLink.href = resourceUrl;
  downloadLink.target = "_blank";
  downloadLink.download = getFilenameFromUrl(resourceUrl);

  document.body.appendChild(downloadLink);
  downloadLink.click();

  document.body.removeChild(downloadLink);
}

export function getFilenameFromUrl(url) {
  return url.substring(url.lastIndexOf("/") + 1);
}