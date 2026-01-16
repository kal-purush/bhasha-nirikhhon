interface Translate {
  readonly attribute: string;
  readonly original: string;
  readonly translate: string;
  readonly tag: string;
}

type Problem = { message: string; source: Translate[] };

/**
 * 同じ台詞の行の中から、異なる訳が当てられている行を検索します。
 */
export function checkDuplicateTranslate(translates: Translate[]): Problem[] {
  const groups = translates.reduce<{ [key: string]: Translate[] }>((dic, curr) => {
    if (curr.original) {
      const key = `${curr.attribute} ${curr.original}`;
      if (!dic[key]) dic[key] = [curr];
      else dic[key].push(curr);
    }
    return dic;
  }, {});
  return Object.keys(groups)
    .map(k => groups[k])
    .filter(g => g.length > 1)
    .filter(g => g.map(t => t.translate).some((t, _, a) => t != a[0]))
    .map(g => ({ message: '同じ台詞に異なる翻訳が当てられています。', source: g }));
}