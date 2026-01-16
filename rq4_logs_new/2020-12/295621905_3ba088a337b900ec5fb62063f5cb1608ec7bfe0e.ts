import { Ambiguous, Kind } from '@jamashita/publikum-type';
import { RuntimeError } from './RuntimeError';

export abstract class Errors<N extends string = string> extends RuntimeError<N> {
  private readonly errors: Array<Error>;

  private static getMessage(errors: Array<Error>): string {
    return errors.map<string>((error: Error) => {
      return error.message;
    }).join('\n');
  }

  protected constructor(errors: Array<Error>) {
    super(Errors.getMessage(errors));
    this.errors = errors;
  }

  public getStack(): string {
    return this.errors.map<Ambiguous<string>>((error: Error) => {
      return error.stack;
    }).filter((stack: Ambiguous<string>): stack is string => {
      return !Kind.isUndefined(stack);
    }).join('\n');
  }
}