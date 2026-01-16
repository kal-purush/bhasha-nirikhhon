const myFunc = require('./core')

describe('core', () => {
  const fileName = './sample.json'

  it('should create a CSV', async () => {
    const output = await myFunc(fileName)
    expect(output).toEqual(`BillOfLading,Invoice,ItemNumber,Quantity
bill1,inv1,item1,1
bill1,inv1,item2,2
bill2,inv2,item3,3
`)
  })

  it('should create reverse csv', async () => {
    const output = await myFunc(fileName, { reverse: true })
    expect(output).toEqual(`BillOfLading,Invoice,ItemNumber,Quantity
bill2,inv2,item3,3
bill1,inv1,item2,2
bill1,inv1,item1,1
`)
  })

  it('should do caps', async () => {
    const output = await myFunc(fileName, { caps: true })
    expect(output).toEqual(`BillOfLading,Invoice,ItemNumber,Quantity
BILL1,INV1,ITEM1,1
BILL1,INV1,ITEM2,2
BILL2,INV2,ITEM3,3
`)
  })

  it('should fail on no bill of lading number', async () => {
    expect.hasAssertions()
    try {
      await myFunc('./bad.json')
    } catch (e) {
      expect(e.message).toMatch(/bol_number/)
    }
  })
})