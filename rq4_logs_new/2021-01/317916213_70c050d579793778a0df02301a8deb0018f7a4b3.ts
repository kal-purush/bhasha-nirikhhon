import PcModel from ".."

import takeDamage from "./takeDamage"

describe('PlayerCharacter.takeDamage()', () => {
  const mockPc = () => PcModel.createCharacter({
    name: "testName",
  })

  it('works for guard = 0/dodge = 0', () => {
    const pc = mockPc()
    const damageTakenReturned = takeDamage.call(pc, 7)
    const actualDamageTaken = pc.hp.max - pc.hp.current
    expect(damageTakenReturned).toBe(7)
    expect(actualDamageTaken).toBe(7)
  })
  it('works for guard <= 0 and/or dodge <= 0', () => {
    const pc = mockPc()
    pc.guard.value = -3
    pc.guard.bonus = -1
    pc.dodge.value = -1
    pc.dodge.bonus = -2
    const damageTakenReturned = takeDamage.call(pc, 6)
    const actualDamageTaken = pc.hp.max - pc.hp.current
    expect(damageTakenReturned).toBe(6)
    expect(actualDamageTaken).toBe(6)
  })
  it('works for guard > 0', () => {
    const pc = mockPc()
    pc.guard.value = 1
    pc.guard.bonus = 2
    pc.dodge.value = -1
    pc.dodge.bonus = -2
    const damageTakenReturned = takeDamage.call(pc, 6)
    const actualDamageTaken = pc.hp.max - pc.hp.current
    expect(damageTakenReturned).toBe(3)
    expect(actualDamageTaken).toBe(3)
  })
  it('works for dodge > 0', () => {
    const pc = mockPc()
    pc.guard.value = 0
    pc.guard.bonus = 0
    pc.dodge.value = 3
    pc.dodge.bonus = 2
    const damageTakenReturned = takeDamage.call(pc, 8)
    const actualDamageTaken = pc.hp.max - pc.hp.current
    expect(damageTakenReturned).toBe(6)
    expect(actualDamageTaken).toBe(6)
  })
  it('works for dodge > 0 and guard > 0', () => {
    const pc = mockPc()
    pc.guard.value = 1
    pc.guard.bonus = 2
    pc.dodge.value = 3
    pc.dodge.bonus = 7
    const damageTakenReturned = takeDamage.call(pc, 10)
    const actualDamageTaken = pc.hp.max - pc.hp.current
    expect(damageTakenReturned).toBe(3)
    expect(actualDamageTaken).toBe(3)
  })
  it('works with temporary hp', () => {
    // case the damage goes through all of the temp hp
    const pc1 = mockPc()
    const tempHp1 = pc1.hp.temporary = 5
    pc1.guard.value = 1
    pc1.guard.bonus = 2
    pc1.dodge.value = 3
    pc1.dodge.bonus = 2
    const damageTakenReturned1 = takeDamage.call(pc1, 20)

    const tempHpDamageTaken1 = tempHp1 - pc1.hp.temporary
    const hpDamageTaken1 = pc1.hp.max - pc1.hp.current
    expect(pc1.hp.max).toBe(10)
    expect(pc1.hp.current).toBe(3)
    expect(pc1.hp.temporary).toBe(0)
    expect(damageTakenReturned1).toBe(12)
    expect(tempHpDamageTaken1).toBe(5)
    expect(hpDamageTaken1).toBe(7)
    
    // case the damage doesn't "breat" the temp hp
    const pc2 = mockPc()
    const tempHp2 = pc2.hp.temporary = 30
    pc2.guard.value = 1
    pc2.guard.bonus = 2
    pc2.dodge.value = 3
    pc2.dodge.bonus = 2
    const damageTakenReturned2 = takeDamage.call(pc2, 20)

    const tempHpDamageTaken2 = tempHp2 - pc2.hp.temporary
    const hpDamageTaken2 = pc2.hp.max - pc2.hp.current
    expect(pc2.hp.max).toBe(10)
    expect(pc2.hp.current).toBe(10)
    expect(pc2.hp.temporary).toBe(18)
    expect(damageTakenReturned2).toBe(12)
    expect(tempHpDamageTaken2).toBe(12)
    expect(hpDamageTaken2).toBe(0)
  })
})