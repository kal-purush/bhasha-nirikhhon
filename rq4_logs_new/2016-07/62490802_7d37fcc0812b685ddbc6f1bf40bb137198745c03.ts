import {autoinject} from 'aurelia-framework';
import {NPMAPI}     from '../../shared/npm-api';
import {FS, NPM}    from 'monterey-pal';
import * as semver  from 'semver';

@autoinject()
export class Analyzer {
  constructor(private npmAPI: NPMAPI) {
  }

  async analyze(project) {
    let packageJSON = JSON.parse(await FS.readFile(project.packageJSONPath));
    let deps = Object.assign({}, packageJSON.dependencies, packageJSON.devDependencies);
    let topLevelDependencies = [];

    // normalize data for the grid
    let keys = Object.keys(deps);
    for (let i = 0; i < keys.length; i++) {
      let key = keys[i];
      let dep = deps[key];

      topLevelDependencies.push({
        name: key,
        range: deps[key]
      });
    }

    return topLevelDependencies;
  }

  async lookupInstalledVersions(project, topLevelDependencies) {
    let json = await NPM.ls({ workingDirectory: FS.getFolderPath(project.packageJSONPath) });
    let keys = Object.keys(json.dependencies);

    for (let i = 0; i < keys.length; i++) {
      let key = keys[i];
      let tld = topLevelDependencies.find(x => x.name === key);
      if (tld) {
        tld.version = json.dependencies[key].version;
      }
    }
  }

  getLatestVersions(dependencies) {
    dependencies.forEach(dep => {
      this.npmAPI.getLatest(dep.name)
      .then(version => dep.latest = version)
    });
  }

  async checkIfUpToDate(m) {
    // not sure how to handle this yet
    // "typescript": ">=1.9.0-dev || ^2.0.0",
    if (m.version.indexOf('||') > -1) return;

    if (semver.lt(m.version, m.latest)) {
      m.isUpToDate = false;
    } else {
      m.isUpToDate = true;
    }
  }
}