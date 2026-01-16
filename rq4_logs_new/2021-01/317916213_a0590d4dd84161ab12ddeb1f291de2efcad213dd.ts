import PcModel from ".."

import shortRest from "./shortRest"

describe('PlayerCharacter.shortRest()', () => {
  it('works by default', () => {
    const pc = PcModel.createCharacter({ name: "test", level: 10 })
    pc.hp.current = 0
    pc.mp.current = 0
    pc.attributes[pc.hpDiceAttb].value = 6
    pc.attributes[pc.hpDiceAttb].bonus = 4
    pc.attributes[pc.mpDiceAttb].value = 2
    pc.attributes[pc.mpDiceAttb].bonus = 3
    
    const recovered = shortRest.call(pc)

    expect(recovered.hp).toBe(0)
    expect(pc.hp.current).toBe(0)
    expect(recovered.mp).toBe(pc.mp.current)
  })

  it(`doesn't heal above max`, () => {
    const pc = PcModel.createCharacter({ name: "test", level: 10 })
    pc.mp.current = pc.mp.max - 1
    pc.attributes[pc.mpDiceAttb].value = 20
    pc.attributes[pc.mpDiceAttb].bonus = 3
    const recovered = shortRest.call(pc)
    expect(recovered.mp).toBe(1)
    expect(pc.mp.current).toBe(pc.mp.max)
  })
})