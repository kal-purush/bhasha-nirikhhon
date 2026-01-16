import type { Config } from "@jest/types"

const config: Config.InitialOptions = {
  preset: "ts-jest",
  testEnvironment: "jsdom",
  // transform: {
  //   "node_modules/react/.+\\.tsx?$": "ts-jest"
  // },
  transformIgnorePatterns: ["node_modules/(?!react/.*)"],
  moduleNameMapper: {
    ".+\\.(css|sass|scss)$": "identity-obj-proxy",
    ".+\\.(jpg|jpeg|png|gif|eot|otf|webp|svg|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga)$":
      "<rootDir>/__mocks__/fileMock.ts",
    "^components/(.*)$": "<rootDir>/src/components/$1",
    "^lib/(.*)$": "<rootDir>/src/lib/$1",
    "^images/(.*)$": "<rootDir>/src/images/$1",
    "^pages/(.*)$": "<rootDir>/src/pages/$1",
    "^styles/(.*)$": "<rootDir>/src/styles/$1",
  },
  setupFilesAfterEnv: ["<rootDir>/jest.setup.ts"]
}
export default config