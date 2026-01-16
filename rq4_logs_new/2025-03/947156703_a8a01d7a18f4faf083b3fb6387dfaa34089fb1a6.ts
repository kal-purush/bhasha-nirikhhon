type MardownType =
  | "title"
  | "subtitle"
  | "paragraph"
  | "link"
  | "code"
  | "image";
type IParsedData = {
  textType: MardownType;
  content: string;
};

interface IMarkdownParserConstructor {
  rawData: string;
  nextLineCharacter: "\r\n" | "\n";
}

interface IMarkdownParserProperties extends IMarkdownParserConstructor {
  getParsedData: () => IParsedData[];
  rawData: string;
}

class MarkdownParser implements IMarkdownParserProperties {
  rawData: string;
  nextLineCharacter: "\r\n" | "\n";
  constructor(props: IMarkdownParserConstructor) {
    this.rawData = props.rawData;
    this.nextLineCharacter = props.nextLineCharacter;
  }

  /**
   * Parse data and class each string bloc
   * @returns parsed data
   */
  getParsedData = (): IParsedData[] => {
    const lines = this.rawData.split(this.nextLineCharacter);
    const parsedData: IParsedData[] = [];
    lines.forEach((line) => {
      if (!line) return;
      const [textType, content] = this._getTextType(line);
      parsedData.push({
        textType,
        content,
      });
    });
    console.log(parsedData);
    return [];
  };

  /**
   * Get text type and new string
   * @param line line data
   */
  _getTextType = (line: string): [MardownType, string] => {
    if (line.startsWith("##")) {
      return ["subtitle", line.replaceAll("#", "")];
    } else if (line.startsWith("#")) {
      return ["title", line.replaceAll("#", "")];
    } else {
      return ["paragraph", line];
    }
  };
}
export default MarkdownParser;