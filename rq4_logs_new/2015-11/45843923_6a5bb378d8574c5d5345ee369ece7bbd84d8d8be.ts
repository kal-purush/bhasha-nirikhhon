import {Enum} from './enum';

export class Orientation extends Enum {
  static HORIZONTAL:Orientation = new Orientation('horizontal');
  static VERTICAL:Orientation = new Orientation('vertical');
}

export class HorizontalAlignment extends Enum {
  static LEFT:HorizontalAlignment = new HorizontalAlignment('left');
  static RIGHT:HorizontalAlignment = new HorizontalAlignment('right');
  static CENTER:HorizontalAlignment = new HorizontalAlignment('center');
}

export class VerticalAlignment extends Enum {
  static TOP:VerticalAlignment = new VerticalAlignment('top');
  static BOTTOM:VerticalAlignment = new VerticalAlignment('bottom');
  static MIDDLE:VerticalAlignment = new VerticalAlignment('middle');
}