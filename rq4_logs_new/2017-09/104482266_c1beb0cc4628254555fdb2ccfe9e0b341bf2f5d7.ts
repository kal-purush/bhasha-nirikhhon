import * as L from "leaflet";

export class Element extends L.FeatureGroup {
  /**
   * Unique id.
   */
  public id: string;

  private _selectable: boolean = false;
  private _selected: boolean = false;

  private _styleFunction: (element: Element) => L.PathOptions = element => ({
    color: element.selected ? "#f00" : "#666",
    weight: element.selected ? 2 : 0.5,
    opacity: 1,
    fillColor: element.selected ? "#fbb" : "#eee",
    fillOpacity: 0.7
  });

  /**
   * An element has one or more paths and can be selected and styled.
   * Extends L.FeatureGroup which provides interaction events.
   */
  public constructor(
    id: string,
    paths: L.Path[],
    options: IElementOptions = {}
  ) {
    super(paths);

    this.id = id;

    if (options.selectable !== undefined) {
      this.selectable = options.selectable;
    }

    if (options.selected !== undefined) {
      this._selected = options.selected;
    }

    if (options.styleFunction !== undefined) {
      this._styleFunction = options.styleFunction;
    }

    this.updateStyle();
  }

  /**
   * Function that is used to update the path styles for the element.
   * Used in the constructor and whenever updateStyles() is called.
   */
  public set styleFunction(styleFunction: (element: Element) => L.PathOptions) {
    this._styleFunction = styleFunction;
    this.updateStyle();
  }

  /**
   * Whether the element can be selected.
   * If true, clicking the element selects or deselects it.
   */
  public set selectable(value: boolean) {
    this._selectable = value;

    const onClick = () => {
      this.selected = !this.selected;
    };

    if (value) {
      this.on("click", onClick);
    } else {
      this.off("click", onClick);
    }
  }

  public get selectable(): boolean {
    return this._selectable;
  }

  /**
   * Whether the element is currently selected.
   * Assigning a value fires the 'select' and 'deselect' events.
   */
  public get selected(): boolean {
    return this._selected;
  }

  public set selected(value: boolean) {
    if (this.selectable) {
      if (value && !this._selected) {
        this._selected = true;
        this.fire("select", this);
        this.updateStyle();
      }
      if (!value && this.selected) {
        this._selected = false;
        this.fire("deselect", this);
        this.updateStyle();
      }
    }
  }

  /**
   * Updates the path styles using the current styleFunction.
   */
  public updateStyle(): this {
    const style = this._styleFunction(this);
    this.eachLayer(layer => {
      if (layer instanceof L.Path) {
        layer.setStyle(style);
      }
    });
    return this;
  }
}

export interface IElementOptions {
  selectable?: boolean;
  selected?: boolean;
  styleFunction?: (element: Element) => L.PathOptions;
}