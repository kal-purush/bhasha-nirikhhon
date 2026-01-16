const TAB_SPACES = 2;

export default class StringUtil {
  public static getLineInfo(pos: number, str: string): LineInfo {
    return getLineInfo_(pos, str);
  }

  public static changeIndent(n: number, content: string, line: LineInfo): string {
    if (n > 0) {
      return content.substring(0, line.posBegin) + ' '.repeat(n) + content.substring(line.posBegin);
    } else if (n < 0) {
      n = -n;
      if (n > line.indent) {
        throw new Error();
      }
      return content.substring(0, line.posBegin) + content.substring(line.posBegin + n);
    }
    return content;
  }

  // インデントを一段上げた文字列を返す
  public static increaseIndent(content: string, pos: number): {updated: string, numAdd: number} {
    const line = getLineInfo_(pos, content);
    const numAdd: number = TAB_SPACES - (line.indent % TAB_SPACES);
    return {
      updated: StringUtil.changeIndent(numAdd, content, line),
      numAdd,
    };
  }

  // インデントを一段下げた文字列を返す
  public static decreaseIndent(content: string, pos: number): {updated: string, numRemove: number} {
    const line = getLineInfo_(pos, content);
    if (line.indent === 0) {
      return {
        updated: content,
        numRemove: 0,
      };
    }
    const r = line.indent % TAB_SPACES;
    const numRemove = (r === 0) ? TAB_SPACES : r;
    return {
      updated: StringUtil.changeIndent(-numRemove, content, line),
      numRemove,
    };
  }

  public static increaseIndentRange(str: string, posStart: number, posEnd: number): {updated: string, numAddStart: number, numAddEnd: number} {
    const ret = changeIndentRangeInner_(str, posStart, posEnd, true);
    return {
      updated: ret.updated,
      numAddStart: ret.diffStart,
      numAddEnd: ret.diffEnd,
    };
  }

  public static decreaseIndentRange(str: string, posStart: number, posEnd: number): {updated: string, numRemoveStart: number, numRemoveEnd: number} {
    const ret = changeIndentRangeInner_(str, posStart, posEnd, false);
    return {
      updated: ret.updated,
      numRemoveStart: ret.diffStart,
      numRemoveEnd: ret.diffEnd,
    };
  }
}

export interface LineInfo {
  str: string;
  posBegin: number;
  posEnd: number; // (index of the last character of str) + 1
  col: number;
  num: number;    // 行番号(1-)
  indent: number;
  bullet: Bullet;
}

export type Bullet = '' | '* ' | '- ' | '・';

function getLineInfo_(pos: number, str: string): LineInfo {
  if (pos > str.length) {
    throw new Error('invalid cursor position');
  }
  let posBegin: number = pos;
  while (posBegin > 0) {
    if (str.charAt(posBegin - 1) === '\n') {
      break;
    }
    posBegin--;
  }
  let posEnd: number = str.indexOf('\n', pos);
  if (posEnd === -1) {
    posEnd = str.length;
  }
  const line: string = str.slice(posBegin, posEnd);
  let indent: number = 0;
  for (; indent < line.length; indent++) {
    if (line.charAt(indent) !== ' ') {
      break;
    }
  }
  // seek bullet
  let bulletFound = '';
  ['* ', '- ', '・'].forEach(bullet => {
    if (line.length >= indent + bullet.length && line.substr(indent, bullet.length) === bullet) {
      bulletFound = bullet;
    }
  });
  return {
    str: line,  // should not include \n
    posBegin,
    posEnd,
    col: pos - posBegin,
    num: str.substring(0, pos).split('\n').length,
    indent,
    bullet: bulletFound as Bullet,
  };
}

function changeIndentRangeInner_(
  str: string,
  posStart: number,
  posEnd: number,
  isIncrease: boolean): {updated: string, diffStart: number, diffEnd: number} {
  const lineStart = getLineInfo_(posStart, str);
  const lineEnd = getLineInfo_(posEnd, str);
  let line = getLineInfo_(posStart, str);
  let diffStart = 0;
  let diffEnd = 0;
  while (true) {  // eslint-disable-line no-constant-condition
    let diff: number = 0;
    if (isIncrease) {
      const ret = StringUtil.increaseIndent(str, line.posBegin);
      str = ret.updated;
      diff = ret.numAdd;
    } else {
      const ret = StringUtil.decreaseIndent(str, line.posBegin);
      str = ret.updated;
      diff = ret.numRemove;
    }
    if (line.num === lineStart.num) {
      diffStart += diff;
    }
    diffEnd += diff;
    if (line.num === lineEnd.num) {
      break;
    }
    line = isIncrease   // next line
      ? getLineInfo_(line.posEnd + diff + 1, str)
      : getLineInfo_(line.posEnd - diff + 1, str);
  }
  return {
    updated: str,
    diffStart,
    diffEnd,
  };
}