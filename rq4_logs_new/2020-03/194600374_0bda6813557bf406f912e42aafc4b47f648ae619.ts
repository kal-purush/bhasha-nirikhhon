import { assert } from "chai";
import { getNewText } from "../src/split-multiple-language"

describe("getNewText", () => {
  context("with complex text", () => {
    it("returns valid text by index 3", () => {
      const { newIndex, newIndexChanged, newText } = getNewText(1, false, "業務:|施設訪問|||監査を行い、科学性・倫理性の確認|と臨床研究の質向上のための教育|||を行う。", 3)
      assert.equal(newIndex, 1)
      assert.equal(newIndexChanged, true)
      assert.equal(newText, '業務:施設訪問監査を行い、科学性・倫理性の確認と臨床研究の質向上のための教育を行う。')
    })
    it("returns valid text by index 2", () => {
      const { newIndex, newIndexChanged, newText } = getNewText(1, false, "業務:|施設訪問|||監査を行い、科学性・倫理性の確認|と臨床研究の質向上のための教育|||を行う。", 2)
      assert.equal(newIndex, 1)
      assert.equal(newIndexChanged, true)
      assert.equal(newText, '業務:監査を行い、科学性・倫理性の確認を行う。')
    })
  })
})