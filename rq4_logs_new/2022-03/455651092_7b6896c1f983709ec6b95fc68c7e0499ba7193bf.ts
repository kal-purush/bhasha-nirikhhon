import { Transform, TransformFnParams } from 'class-transformer';

export function TransformInt() {
  return Transform((param: TransformFnParams) => {
    return parseInt(param.value, 10) || undefined;
  });
}