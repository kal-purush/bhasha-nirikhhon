import PcModel from "Models/PlayerCharacter"
import { useDbConnection } from "Utils/mongoTest"
import ResourceModel from ".."

import createHp from "./createHp"

describe('PcResource.createHp()', () => {
  useDbConnection("PcResource_createHp")
  
  it("works", () => {
    const pc1 = PcModel.createCharacter({ name: "test", level: 1 })
    const hp1 = createHp.call(ResourceModel, pc1)
    expect(hp1.max).toBe(10)
    expect(hp1.base_max).toBe(10)
    expect(hp1.bonus_max).toBe(0)
    expect(hp1.current).toBe(10)

    const pc2 = PcModel.createCharacter({ name: "test", level: 1 })
    pc2.hpScaling.bonus = 3             // hp +3
    pc2.level += 2                      // hp +4
    pc2.attributes.Fortitude.value = 3  // hp +6
    const hp2 = createHp.call(ResourceModel, pc2)
    expect(hp2.max).toBe(23)
    expect(hp2.base_max).toBe(23)
    expect(hp2.bonus_max).toBe(0)
    expect(hp2.current).toBe(23)
  })
})