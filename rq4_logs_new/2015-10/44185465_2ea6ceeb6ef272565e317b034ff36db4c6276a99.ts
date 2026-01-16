interface IKakFile {
  path:    string;
  content: string;
}

interface IKakMainElement extends polymer.Base, HTMLElement {
  openFile(file: IKakFile): void;
}