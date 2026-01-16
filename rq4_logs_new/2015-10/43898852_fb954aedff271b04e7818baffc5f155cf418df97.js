ReactiveMap = class ReactiveMap {
  constructor(map) {
    this.keys = {};
    this.keyDeps = {};
    this.keyValueDeps = {};
    this.allDeps = new Tracker.Dependency;

    this.set(map);
  }

  set(key, value) {
    if (_.isObject(key))
      return _.each(key, (value, key) => this.set(key, value));

    check(key, String);

    let old_value = this.keys[key],
        is_new = (value !== old_value || !isPrimitive(old_value));

    this.keys[key] = value;

    if (is_new) {
      changed(this.keyDeps[key]);

      let deps = this.keyValueDeps[key];

      if (deps) {
        changed(deps[old_value]);
        changed(deps[value]);
      }
    }

    if (is_new || _.isUndefined(old_value))
      changed(this.allDeps);
  }

  setDefault(key, value) {
    if (_.isObject(key))
      return _.each(key, (value, key) => this.setDefault(key, value));

    check(key, String);

    if (_.isUndefined(this.keys[key]))
      this.set(key, value);
  }

  _ensureKey(key) {
    if (_.isUndefined(this.keyDeps[key])) {
      this.keyDeps[key] = new Tracker.Dependency;
      this.keyValueDeps[key] = {};
    }
  }

  get(key) {
    check(key, String);

    this._ensureKey(key);
    this.keyDeps[key].depend();

    return this.keys[key];
  }

  equals(key, value) {
    check(key, String);

    if (Tracker.active) {
      this._ensureKey(key);

      let deps = this.keyValueDeps[key];

      if (_.isUndefined(deps[value]))
        deps[value] = new Tracker.Dependency;

      let is_new = deps[value].depend();

      if (is_new) {
        Tracker.onInvalidate(() => {
          if (!deps[value].hasDependents())
            delete deps[value];
        });
      }
    }

    return value === this.keys[key];
  }

  remove(key) {
    check(key, String);

    let old_value = this.keys[key];

    if (!_.isUndefined(old_value)) {
      delete this.keys[key];

      changed(this.keyDeps[key]);

      let deps = this.keyValueDeps[key];

      if (deps)
        changed(deps[old_value]);

      changed(this.allDeps);

      return true;
    }

    return false;
  }

  all() {
    this.allDeps.depend();

    return this.keys;
  }

  clear() {
    _.each(this.keys, (value, key) => this.remove(key));

    changed(this.allDeps);
  }
};


function isPrimitive(value) {
  return _.isString(value) || _.isBoolean(value) || _.isNumber(value) || _.isNull(value);
}

function changed(dep) {
  dep && dep.changed();
}