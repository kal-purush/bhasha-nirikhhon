import {FS} from 'monterey-pal';

export class JSPMDetection {
  async findJspmConfig(project) {
    let packageJSON = JSON.parse(await FS.readFile(project.packageJSONPath));
    let isUsingJSPM = false;
    let configJs = null;

    if (packageJSON.jspm) {
      isUsingJSPM = true;

      await this.findJspmVersion(project, packageJSON);
      let major = parseInt(project.jspmVersion.split('.')[0], 10);
      let minor = parseInt(project.jspmVersion.split('.')[1], 10);
      if (major === 0) {
        if (minor < 17) {
          let config = this.readJspm016(project, packageJSON);
          project.configJsPath = config;
        } else {
          let config = this.readJspm017(project, packageJSON);
          project.configJsPath = config;
        }
      }
    }

    project.isUsingJSPM = isUsingJSPM;
  }

  readJspm016(project, packageJSON) {
    let baseURL = '.';
    if (packageJSON.jspm.directories && packageJSON.jspm.directories.baseURL) {
      baseURL = packageJSON.jspm.directories.baseURL;
    }
    return FS.join(project.path, baseURL, 'config.js');
  }

  readJspm017(project, packageJSON) {
    // TODO: implement reading JSPM 0.17.x configuration
    let baseURL = '';
    if (packageJSON.jspm.directories && packageJSON.jspm.directories.baseURL) {
      baseURL = packageJSON.jspm.directories.baseURL;
    }
    return FS.join(project.path, baseURL, 'jspm.config.js');
  }

  async findJspmVersion(project, packageJSON) {
    let jspmDefinition = (packageJSON.dependencies && packageJSON.dependencies.jspm) || (packageJSON.devDependencies && packageJSON.devDependencies.jspm);
    let jspmVersion = null;
    if (jspmDefinition) {
      jspmVersion = jspmDefinition;
      if (jspmVersion[0] === '^' || jspmVersion[0] === '~') {
        // TODO: find version actually used
        jspmVersion = jspmVersion.substring(1);
      }
      project.jspmDefinition = jspmDefinition;
      project.jspmVersion = jspmVersion;
    } else {
      // TODO: JSPM not found in package.json dependencies - throw error?
    }
  }
}