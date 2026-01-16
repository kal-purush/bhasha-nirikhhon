export class Analyzer {

  loader;
  packageJSON;

  analyze(loader, packageJSON) {
    this.loader = loader;
    this.packageJSON = packageJSON;

    this.lookupRanges();

    return this.normalize();
  }

  normalize() {
    let normalized = [];
    let baseMap = this.loader.baseMap;
    let depMap = this.loader.depMap;

    // the map of all dependencies (not just top level dependencies) does
    // not contain any information if it's also a top level dependency
    // more importantly, the semver range of the top level dependency
    // here we look that up through the map of top level dependencies
    let keys = Object.keys(baseMap);
    for (let i = 0; i < keys.length; i++) {
      let packageName = keys[i];
      let pkg = baseMap[packageName];

      pkg.isTopLevel = true;

      if (depMap[pkg.exactName]) {
        Object.assign(depMap[pkg.exactName], pkg);
      }
    }

    keys = Object.keys(depMap);
    for (let i = 0; i < keys.length; i++) {

      // make sure that every dependency map has a 'package' name
      if (!depMap[keys[i]].package) {
        depMap[keys[i]].package = keys[i];
      }

      normalized.push(depMap[keys[i]]);
    }

    // return an array of all dependencies
    return normalized;
  }

  lookupRanges() {
    let baseMap = this.loader.baseMap;
    let depMap = this.loader.depMap;

    // first the top level dependencies
    let keys = Object.keys(baseMap);
    for (let i = 0; i < keys.length; i++) {
      let packageName = keys[i];
      let baseMapDep = baseMap[packageName];

      let packageJSONDep = this.packageJSON.jspm.dependencies[packageName];

      if (!packageJSONDep) {
        packageJSONDep = this.packageJSON.jspm.devDependencies[packageName];
      }

      if (packageJSONDep) {
        baseMapDep.range = this.extractRange(packageJSONDep);
      }
    }
  }

  extractRange(packageName) {
    // packageName could be github:systemjs/plugin-text@^0.0.8
    // we want the ^0.0.8 part
    return packageName.split('@')[1];
  }
}