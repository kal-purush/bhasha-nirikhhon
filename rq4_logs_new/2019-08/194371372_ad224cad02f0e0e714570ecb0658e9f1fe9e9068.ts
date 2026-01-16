export default function getApiUrl(): string {
  if (window.location.host === "github.com") {
    return "api.github.com";
  } else {
    return `${window.location.host}/api/v3`;
  }
}