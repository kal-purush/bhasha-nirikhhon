import resolveReferences from 'self-referenced-object';
export default class {
  constructor(options) {
    this.defaultOptions = options;
  }

  addPlanOptions = (options) =>
    resolveReferences({
      ...this.defaultOptions,
      ...options,
    });

  async buildSvg(options) {
    throw new Error('nyi');
  }
}