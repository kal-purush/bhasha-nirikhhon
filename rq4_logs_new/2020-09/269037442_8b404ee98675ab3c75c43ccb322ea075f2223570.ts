import { expect } from 'chai'
import {
        /* Base */
        Entry, KoconutPrimitive, Pair,
        /* Container */
        KoconutArray, KoconutSet, KoconutMap
} from "../internal"

describe(`${KoconutMap.name} -- Property Getter`, () => {

    it(KoconutMap.prototype.keys.name, async () => {

        const koconut = KoconutMap.of(
                        [0, 'a'],
                        [1, 'b'],
                        [2, 'c'])
        
        const yieldable =
                        koconut
                        .keys()
        expect(yieldable).to.be.instanceOf(KoconutSet)
        const result = await yieldable.yield()
        expect(result).eqls(new Set([0,1,2]))

    })
    
    it(KoconutMap.prototype.entries.name, async () => {

        const koconut = KoconutMap.of(
                        [0, 'a'],
                        [1, 'b'],
                        [2, 'c'])

        const yieldable =
                        koconut
                        .entries()
        expect(yieldable).to.be.instanceOf(KoconutSet)
        const result = await yieldable.yield()
        const expectedResultEntryArray = [
                                            new Entry(0, 'a'),
                                            new Entry(1, 'b'),
                                            new Entry(2, 'c')
                                        ]
        expect(result).eqls(new Set(expectedResultEntryArray))

    })

    it(KoconutMap.prototype.values.name, async () => {

        const koconut = KoconutMap.of(
                        [0, 'a'],
                        [1, 'b'],
                        [2, 'c'])

        const yieldable =
                        koconut
                        .values()
        expect(yieldable).to.be.instanceOf(KoconutArray)
        const result = await yieldable.yield()
        expect(result).eqls(['a','b','c'])

    })

    it(KoconutMap.prototype.size.name, async () => {

        const koconut = KoconutMap.of(
                        [0, 'a'],
                        [1, 'b'],
                        [2, 'c'])

        const yieldable =
                        koconut
                        .size()
        expect(yieldable).to.be.instanceOf(KoconutPrimitive)
        const result = await yieldable.yield()
        expect(result).equals(3)

    })
    
})

describe(`${KoconutMap.name} -- Function`, () => {

    it(KoconutMap.prototype.all.name, async () => {

        const koconut = KoconutArray.of(1,2,3,4,5)
                        .associate(eachElement => [eachElement, eachElement * 2])

        /* Case 1 */
        const yieldableCase1 = 
                        koconut
                        .all(eachEntry => eachEntry.value / eachEntry.key == 2)
        expect(yieldableCase1).to.be.instanceOf(KoconutPrimitive)
        const resultCase1 = await yieldableCase1.yield()
        expect(resultCase1).equals(true)

        /* Case 2 */
        const yieldableCase2 =
                        koconut
                        .all(eachEntry => eachEntry.value < 8)
        expect(yieldableCase2).to.be.instanceOf(KoconutPrimitive)
        const resultCase2 = await yieldableCase2.yield()
        expect(resultCase2).equals(false)

    })

    it(KoconutMap.prototype.any.name, async () => {

        const koconut = KoconutArray.of(1,2,3,4,5)
                        .associate(eachElement => [eachElement, eachElement * eachElement])

        /* Case 1 */
        const yieldableCase1 =
                        koconut
                        .any(eachEntry => eachEntry.value == 1)
        expect(yieldableCase1).to.be.instanceOf(KoconutPrimitive)
        const resultCase1 = await yieldableCase1.yield()
        expect(resultCase1).equals(true)
        
        /* Case 2 */
        const yieldableCase2 =
                        koconut
                        .any(eachEntry => eachEntry.value < 0)
        expect(yieldableCase2).to.be.instanceOf(KoconutPrimitive)
        const resultCase2 = await yieldableCase2.yield()
        expect(resultCase2).equals(false)


    })

    it(KoconutMap.prototype.asIterable.name, async () => {

        const koconut = KoconutArray.of(1,2,3)
                        .associate(eachElement => new Pair(eachElement, eachElement))

        const yieldable = 
                        koconut
                        .asIterable()
        expect(yieldable).to.be.instanceOf(KoconutPrimitive)
        const result = await yieldable.yield()
        expect(result).eqls(new Set([
                                    new Entry(1, 1),
                                    new Entry(2, 2),
                                    new Entry(3, 3)
                                ])[Symbol.iterator]())

    })

    it(KoconutMap.prototype.asArray.name, async () => {

        const koconut = KoconutArray.of(1,2,3)
                        .associate(eachElement => new Pair(eachElement, eachElement))

        const yieldable =
                        koconut
                        .asArray()
        expect(yieldable).to.be.instanceOf(KoconutArray)
        const result = await yieldable.yield()
        expect(result).eqls([
                                new Entry(1, 1),
                                new Entry(2, 2),
                                new Entry(3, 3)
                            ])

    })

    it(KoconutMap.prototype.contains.name, async () => {

        /* Case 1 */
        const koconutCase1 = KoconutArray.of(1,2,3)
                            .associate(eachelement => new Pair(eachelement, eachelement))

        const yieldableCase1 =
                        koconutCase1
                        .contains(2)
        expect(yieldableCase1).to.be.instanceOf(KoconutPrimitive)
        const resultCase1 = await yieldableCase1.yield()
        expect(resultCase1).equals(true)

        /* */
    })

})