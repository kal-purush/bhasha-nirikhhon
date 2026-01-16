import PcModel from ".."
import updateMaxResources from "./updateMaxResources"

describe('PlayerCharacter.updateMaxResources()', () => {
  it('works', () => {
    const pc = PcModel.createCharacter({ name: "Test" })
    expect(pc.hp.max).toBe(10)
    expect(pc.mp.max).toBe(10)

    pc.hp.current = 3
    pc.mp.current = 2

    // hp/mp +4
    pc.level += 2
    // hp +6
    pc.attributes.Fortitude.value = 3
    // hp/mp +2
    pc.attributes.Will.value = 2
    // mp +3
    pc.attributes.Logic.value = 2
    // hp +3
    pc.hpScaling.bonus = 3
    // hp +6 (+4.5 mp from highest special)
    pc.hpScaling.Protection = 2
    pc.attributes.Protection.value = 3
    // mp -2
    pc.mpScaling.bonus = -2

    // update max resources
    updateMaxResources.call(pc)
    expect(pc.hp.current).toBe(3)
    expect(pc.hp.max).toBe(31)  // 10 + 4 + 6 + 2 + 3 + 6 = 31
    expect(pc.mp.current).toBe(2)
    expect(pc.mp.max).toBe(21)  // 10 + 4 + 2 + 3 + 4.5 - 2 = 21.5, which rounds down to 21

    pc.hp.current = 31
    pc.hpScaling.bonus -= 4

    updateMaxResources.call(pc)
    expect(pc.hp.max).toBe(27)
    expect(pc.hp.current).toBe(27)
  })
})