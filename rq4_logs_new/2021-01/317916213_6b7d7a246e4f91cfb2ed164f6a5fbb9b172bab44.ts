import { useDbConnection } from "Utils/mongoTest"
import PcModel from ".."

import highestMental from "./highestMental"

describe('PcModel.highestMental', () => {
  useDbConnection("Pc_highestMental")
  
  it("works", () => {
    const pc1 = PcModel.createCharacter({ name: "test", level: 1 })
    // highest physical attribute (either fort, agi or might, since they have the same value)
    const highestAttb1 = highestMental.get.call(pc1)
    expect(highestAttb1.bonus).toBe(0)
    expect(highestAttb1.value).toBe(0)
    expect(highestAttb1.name).toMatch(/Learning|Logic|Perception|Will/)
    
    const pc2 = PcModel.createCharacter({ name: "test", level: 1 })
    pc2.attributes.Perception.value = 2
    pc2.attributes.Perception.bonus = 4
    pc2.attributes.Will.value = 1
    pc2.attributes.Will.bonus = 8
    // highest physical attribute (either fort, agi or might, since they have the same value)
    const highestAttb2 = highestMental.get.call(pc2)
    expect(highestAttb2.value).toBe(2)
    expect(highestAttb2.bonus).toBe(4)
    expect(highestAttb2.name).toBe("Perception")
  })
})