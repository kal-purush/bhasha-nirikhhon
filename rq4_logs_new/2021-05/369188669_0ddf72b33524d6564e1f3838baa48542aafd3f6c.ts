import Agnese from "./Agnese";
import {AnyFunction} from "./PreprocessorManager";


export interface PreprocessorBase {
  name?: string;
  fn?: AnyFunction|null;
}

export interface PreprocessableBase {
  preprocessor?: PreprocessorBase;
}



export class Preprocessor implements PreprocessorBase {
  public name?: string;

  public fn: AnyFunction|null = null;


  constructor(obj: any, public agnese: Agnese) {
    if (obj.name !== undefined) {
      this.name = obj.name;
    }
  }

  public getPreprocessor(): AnyFunction|null {
    if (
      typeof this.fn !== "function"
      && this.agnese?.preprocessorManager
      && typeof this.name === 'string'
    ) {
      this.fn = this.agnese.preprocessorManager.getPreprocessor(this.name);
    }
    return this.fn;
  }

  public process(sourceData?: any, value?: any) {
    // If a preprocessor is defined, it will be used in any case.
    if (this.getPreprocessor() && typeof this.fn === "function") {
      //let extraArguments = this.getPreprocessorExtraArguments(mapFieldInfo.preprocessor);
      value = this.fn(sourceData, value/*, ...extraArguments*/);
    }
    return value;
  }
}



export default class Preprocessable implements PreprocessableBase {
  public preprocessor?: Preprocessor;

  constructor(obj: any, public agnese: Agnese) {
    if (obj.preprocessor !== undefined) {
      this.preprocessor = new Preprocessor(obj.preprocessor, this.agnese);
    }
  }

  public preprocess(sourceData?: any, value?: any) {
    // If a preprocessor is defined, it will be used in any case.
    if (this.preprocessor) {
      //let extraArguments = this.getPreprocessorExtraArguments(mapFieldInfo.preprocessor);
      value = this.preprocessor.process(sourceData, value/*, ...extraArguments*/);
    }
    return value;
  }
}