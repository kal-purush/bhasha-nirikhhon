import axios from 'axios';
import url from 'url';
import {defaultOption} from './default';
import {Option} from './types';
import {cutEmptyLines, trimLastEmptyLines} from './utils';

export function parseComment(comment: string): {name: string; option: Option} {
  const [, , name, ...other] = comment.split(' ');

  const optionRaw = other.join('');
  const option: Option = {
    ...defaultOption,
    ...JSON.parse(optionRaw || '{}'),
  };

  return {name, option};
}

export function parseLines(
  lines: string[],
): ([string] | [string, ReturnType<typeof parseComment>])[] {
  return lines.map((line) => {
    if (/^# ignoregen .*$/.test(line)) {
      return [line, parseComment(line)];
    }
    return [line];
  });
}

export function createTemplateURL({
  name,
  option: {src},
}: ReturnType<typeof parseComment>): string {
  return url.resolve(src, `${name}.ignore`);
}

export async function fetchTemplate(url: string) {
  const {data, status, statusText} = await axios.get<string>(url);
  if (status >= 400) throw new Error(statusText);
  return cutEmptyLines(data.split('\n'));
}

export async function extractTemplate([raw, config]: ReturnType<
  typeof parseLines
>[number]): Promise<string[]> {
  if (!config) return [raw];

  const templateURL = createTemplateURL(config);
  return fetchTemplate(templateURL).then((template) => [raw, ...template, '']);
}

export async function extractTemplates(lines: ReturnType<typeof parseLines>) {
  const lineed = await Promise.all(
    lines.map(async (line) => extractTemplate(line)),
  );
  return lineed;
}

export function flatten(lines: string[][]): string[] {
  return trimLastEmptyLines(lines.flat());
}

export async function parse(lines: string[]): Promise<string[]> {
  return extractTemplates(parseLines(lines)).then(flatten);
}