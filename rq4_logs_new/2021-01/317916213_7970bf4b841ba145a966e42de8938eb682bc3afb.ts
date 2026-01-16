import PcModel from ".."
import updateDefenses from "./updateDefenses"

describe('PlayerCharacter.updateDefenses()', () => {
  it('works', () => {
    const pc = PcModel.createCharacter({ name: "Test" })
    expect(pc.guard.value).toBe(0)
    expect(pc.dodge.value).toBe(0)

    pc.guard.bonus = 3
    pc.dodge.bonus = 2

    // guard +3.75
    pc.attributes.Fortitude.value = 5
    // guard +1
    pc.attributes.Might.value = 1
    pc.attributes.Might.bonus = 7
    // dodge +3.75
    pc.attributes.Agility.value = 5
    // dodge +2
    pc.attributes.Perception.value = 4
    
    // update max resources
    updateDefenses.call(pc)
    expect(pc.guard.bonus).toBe(3)
    expect(pc.guard.value).toBe(4)  // 3.75 + 1 = 4.75, which rounds down to 4
    expect(pc.dodge.bonus).toBe(2)
    expect(pc.dodge.value).toBe(6)  // 2 + 3.75 = 5.75, which rounds up to 6

    pc.guardScaling.bonus -= 2
    updateDefenses.call(pc)

    expect(pc.guard.value).toBe(2)
    expect(pc.guard.bonus).toBe(3)
  })
})