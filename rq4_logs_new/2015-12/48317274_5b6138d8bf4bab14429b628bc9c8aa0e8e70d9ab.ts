/// <reference path="../typings/bundle.d.ts" />

export function processIDNotation(element: CheerioElement, $: CheerioStatic): string {
  const html = $(element).text().replace(/\bid:([a-zA-Z][a-zA-Z0-9_\-]+)\b/, (_: string, name: string) => {
    return `<a href="http://profile.hatena.ne.jp/${name}/">id:${name}</a>`;
  });
  return html;
}