import { EventLike } from "../../types";

export interface PointerDetail {
  _scale?: number;
  clientX: number;
  clientY: number;
  event?: EventLike | Touch;
  identifier: string;
  [prop: string]: any;
}

/**
 * A generic pointer.
 * Represents a (virtual) pointer interaction like mouse or touch point input.
 */
class Pointer<ID> {
  constructor(
    /** Associates the pointer with a ui element. */
    public id: ID,
    /** Pointer detail, stores pointer position. */
    public detail: PointerDetail,
    /** The device identifier (e.g. 'mouse' or 'touch'). */
    public device: string,
  ) {}
}

export default Pointer;