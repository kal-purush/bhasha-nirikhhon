// tslint:disable

import { parseHunkHeaderLine, patch } from "../patch"
import * as fs from "fs-extra"

const properReadFileSync = fs.readFileSync
const properWriteFileSync = fs.writeFileSync
const properUnlinkSync = fs.unlinkSync
const properMoveSync = fs.moveSync

describe("parseHunkHeaderLine", () => {
  it("parses hunk header lines", () => {
    expect(parseHunkHeaderLine("@@ -0,0 +1,21 @@")).toEqual({
      original: {
        length: 0,
        start: 0,
      },
      patched: {
        length: 21,
        start: 1,
      },
    })
  })
})

describe("patch", () => {
  let mockFs: null | Record<string, string> = null

  beforeEach(() => {
    mockFs = {
      "other/file.js": `once
upon
a
time
the
end`,
      "patch/file.patch": `