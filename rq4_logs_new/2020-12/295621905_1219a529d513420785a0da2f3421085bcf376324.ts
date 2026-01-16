import { RuntimeError } from './RuntimeError';

export abstract class RuntimeErrors<N extends string = string> extends RuntimeError<N> {
  private readonly errors: Array<RuntimeError>;

  private static getMessage(errors: Array<RuntimeError>): string {
    return errors.map<string>((error: RuntimeError) => {
      return error.message;
    }).join('\n');
  }

  protected constructor(errors: Array<RuntimeError>) {
    super(RuntimeErrors.getMessage(errors));
    this.errors = errors;
  }

  public getStack(): string {
    return this.errors.map<string>((error: RuntimeError) => {
      return error.getStack();
    }).join('\n');
  }
}