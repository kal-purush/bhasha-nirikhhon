/**
 * @module kryo/builtins/bytes
 */

import { BytesType } from "../types/bytes";

export const $Bytes: BytesType = new BytesType({maxLength: Infinity});