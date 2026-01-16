export function parseRegExp(data: string): Promise<RegExp> {
  return new Promise((resolve, reject) => {
    try {
      resolve(parseRegExpSync(data));
    } catch (err) {
      reject(err);
    }
  });
}

export function parseRegExpSync(data: string): RegExp {
  const flags = data.replace(/.*\/([gimy]*)$/, '$1');
  const pattern = data.replace(new RegExp(`^/(.*?)/${flags}$`), '$1');
  return new RegExp(pattern, flags);
}