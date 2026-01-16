import {
  Beautifier,
  Language,
  BeautifierBeautifyData,
  DependencyType,
  NodeDependency,
} from "unibeautify";
import * as readPkgUp from "read-pkg-up";
import options from "./options";
import * as cosmiconfig from "cosmiconfig";
import { utils, lint } from "stylelint";
const { pkg } = readPkgUp.sync({ cwd: __dirname });

export const beautifier: Beautifier = {
  name: "stylelint",
  package: pkg,
  dependencies: [
    {
      type: DependencyType.Node,
      name: "stylelint",
      package: "stylelint",
      homepageUrl: "https://stylelint.io/",
      installationUrl: "https://stylelint.io/user-guide/cli/",
      bugsUrl: "https://github.com/stylelint/stylelint/issues",
      badges: [
        {
          description: "NPM version",
          url: "https://img.shields.io/npm/v/stylelint.svg",
          href: "https://www.npmjs.org/package/stylelint",
        },
        {
          description: "Build Status",
          url: "https://travis-ci.org/stylelint/stylelint.svg?branch=master",
          href: "https://travis-ci.org/stylelint/stylelint",
        },
        {
          description: "Build status",
          url:
            "https://ci.appveyor.com/api/projects/status/o60hlhki49t2333i/branch/master?svg=true",
          href:
            "https://ci.appveyor.com/project/stylelint/stylelint/branch/master",
        },
        {
          description: "NPM Downloads",
          url: "https://img.shields.io/npm/dm/stylelint.svg",
          href: "https://npmcharts.com/compare/stylelint?minimal=true",
        },
        {
          description: "Backers on Open Collective",
          url: "https://opencollective.com/stylelint/backers/badge.svg",
          href:
            "https://github.com/stylelint/stylelint/blob/master/README.md#backers",
        },
        {
          description: "Sponsors on Open Collective",
          url: "https://opencollective.com/stylelint/sponsors/badge.svg",
          href:
            "https://github.com/stylelint/stylelint/blob/master/README.md#sponsors",
        },
      ],
    },
  ],
  options: {
    CSS: options.Style,
    Less: options.Style,
    SCSS: options.Style,
    Sass: options.Style,
    SugarSS: options.Style,
  },
  resolveConfig: ({ filePath, projectPath, dependencies }) => {
    const pathToSearch = filePath ? filePath : projectPath;
    if (!pathToSearch) {
      return Promise.resolve({});
    }
    const explorer = cosmiconfig("stylelint");
    return explorer
      .search(pathToSearch)
      .then((result: any) => {
        return Promise.resolve({
          config: result.config,
        });
      })
      .catch((error: any) => {
        // tslint:disable-next-line:no-console
        console.log(error);
        return Promise.resolve({});
      });
  },
  beautify({
    text,
    options,
    language,
    filePath,
    projectPath,
    dependencies,
    beautifierConfig,
  }: BeautifierBeautifyData) {
    return new Promise<string>((resolve, reject) => {
      const finalOptions =
        beautifierConfig && beautifierConfig.config
          ? beautifierConfig.config
          : { rules: options };

      return lint({
        code: text,
        config: finalOptions,
        cache: false,
        fix: true,
      })
        .then((data: any) => {
          resolve(data.output);
        })
        .catch((err: any) => {
          console.error(err);
          reject(new Error(err.stack));
        });
    });
  },
};
export default beautifier;