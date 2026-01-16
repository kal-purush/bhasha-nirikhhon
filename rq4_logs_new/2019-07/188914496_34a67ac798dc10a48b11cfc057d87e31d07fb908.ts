declare module "@jaames/iro" {
  interface ColorPickerProps {
    width?: number;
    color?: string;
    layout?: object[];
  }

  export class Color {
    public hexString: string;
  }

  export class ColorPicker {
    public constructor(
      htmlElementOrId?: string | HTMLDivElement | null,
      options?: ColorPickerProps
    );

    public on(eventType: string, callback: (color: Color, changes: object) => void): void;

    public off(eventType: string, callback: (color: Color, changes: object) => void): void;

    public resize(width: number);

    public color: Color;

    public width: number;
  }

  export const ui = {
    Wheel: {},
  };
}