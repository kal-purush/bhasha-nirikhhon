export const $required = Symbol('requiredContext');
export const $req = $required;

export function prepare(func) {
  if (typeof func !== 'function')
    return prepareMultiple.call(this, func);

  const required = func[$required];

  const context = typeof required === 'symbol' ?
    this[required]
    : getContext.call(this, func, required);

  return func.bind(context);
}

function prepareMultiple(funcMap) {
  return Object.entries(funcMap).reduce((obj, [key, _func]) => {
    return Object.assign(obj, {
      [key]: prepare.call(this, _func)
    });
  }, {});
}

function getContext(func, required) {
  return required.reduce((_context, _required) => {
    const property = this[_required];

    return Object.assign(_context, {
      [_required]: property
    });
  }, {});
}