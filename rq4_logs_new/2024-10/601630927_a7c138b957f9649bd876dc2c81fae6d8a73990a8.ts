export const nvaHome = "https://test.nva.sikt.no";

export const nvaLanding = (id: string, lang?: string) =>
  new URL(`/registration/${id}`, nvaHome);