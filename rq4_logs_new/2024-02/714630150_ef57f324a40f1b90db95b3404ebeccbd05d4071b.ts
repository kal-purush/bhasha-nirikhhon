import autoprefixer from "autoprefixer"
import cssnano from "cssnano"
import postcssCalc from "postcss-calc"
import postcssCombineDuplicatedSelectors from "postcss-combine-duplicated-selectors"
import type { Config } from "postcss-load-config"
import postcssModules from "postcss-modules"

import { generateShortName } from "./shortname-generator"

const shortNameGenerator = generateShortName()
const shortNameMemo = new Map<string, string>()

const config: Config = {
  plugins: [
    postcssCombineDuplicatedSelectors(),
    postcssCalc({}),
    autoprefixer({
      overrideBrowserslist: [
        "last 2 versions",
        "not dead"
      ]
    }),
    postcssModules({
      generateScopedName: (name, _filename, css) => {
        const hash = `${name}__${css}`
        if (shortNameMemo.has(hash)) {
          return shortNameMemo.get(hash)!
        }
        const shortName = shortNameGenerator.next().value
        shortNameMemo.set(hash, shortName)
        return shortName
      },
      scopeBehaviour: 'local',
    }),
    cssnano(),
  ]
}

export default config