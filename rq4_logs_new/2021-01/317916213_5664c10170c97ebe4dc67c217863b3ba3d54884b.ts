import { useDbConnection } from "@discord-bot/mongo"
import PcModel from ".."

import AC from "./AC"

describe('PcModel.AC', () => {
  useDbConnection("Pc_AC")
  
  it("works with dodge 0", () => {
    const pc = PcModel.createCharacter({ name: "test" })
    pc.dodge.value = 0
    pc.dodge.bonus = 0
    const ac = AC.get.call(pc)
    expect(ac).toBe(8)
  })

  it("works with positive dodge", () => {
    const pc = PcModel.createCharacter({ name: "test" })
    pc.dodge.value = 1
    pc.dodge.bonus = 3
    const ac = AC.get.call(pc)
    expect(ac).toBe(12)
  })

  it("works with negative dodge", () => {
    const pc = PcModel.createCharacter({ name: "test" })
    pc.dodge.value = -4
    pc.dodge.bonus = 2
    const ac = AC.get.call(pc)
    expect(ac).toBe(6)
  })
})