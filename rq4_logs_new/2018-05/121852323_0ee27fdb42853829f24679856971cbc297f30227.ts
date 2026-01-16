export default class Region {
  public elements: HTMLElement[] = [];
  public get size() {
    return this.elements.length;
  }
  public add(element: HTMLElement) {
    this.elements.push(element);
  }
}