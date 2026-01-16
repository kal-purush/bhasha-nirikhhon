import ResourceModel, { Resource } from "./index"

import { useDbConnection } from "Utils/mongoTest"

describe("PcResource", () => {
  
  useDbConnection("PcResource")

  const hpProps: Resource = {
    base_max: 2,
    bonus_max: 1,
    current: 3,
    temporary: 0,
  }

  describe("CRUD", () => {
    it("creates", async () => {
      const hp = new ResourceModel(hpProps)
      expect(hp.max).toBe(3)  // virtual property
      expect(hp.base_max).toBe(2)
      expect(hp.current).toBe(3)
      expect(hp.temporary).toBe(0)
    })

    it("reads", async () => {
      // create and save
      const hp = new ResourceModel(hpProps)
      await hp.save()

      // read resource
      const readResource = await ResourceModel.findOne({ _id: hp._id })

      expect(readResource?.max).toBe(hp.max)
      expect(readResource?.current).toBe(hp.current)
      expect(readResource?._id).toStrictEqual(hp._id)
    })
    
    it("updates", async () => {
      const hp = new ResourceModel(hpProps)
      await hp.save()
      
      expect(hp?.max).toBe(3)
      expect(hp?.current).toBe(3)

      hp.current -= 2
      await hp.save()

      const readResource = await ResourceModel.findOne({ _id: hp._id })

      expect(readResource?.max).toBe(3)
      expect(readResource?.current).toBe(1)
    })
    
    it("deletes", async () => {
      const hp = new ResourceModel(hpProps)
      await hp.save()
      
      const fetchedResource = await ResourceModel.findOne({ _id: hp._id })
      expect(fetchedResource).not.toBe(null)

      await hp.deleteOne()
      
      const deletedResource = await ResourceModel.findOne({ _id: hp._id })
      expect(deletedResource).toBe(null)
    })
  })

  describe("virtuals", () => {
    describe(".max", () => {
      it("works", () => {
        const hp = new ResourceModel(hpProps)
        expect(hp.base_max).toBe(2)
        expect(hp.bonus_max).toBe(1)
        expect(hp.max).toBe(3)  // virtual property
        
        hp.bonus_max += 2
        hp.base_max -= 1
        
        expect(hp.base_max).toBe(1)
        expect(hp.bonus_max).toBe(3)
        expect(hp.max).toBe(4)  // virtual property
      })
    })
  })

  describe("statics", () => {
    describe(".createHp()", () => {
      it("works", async () => {
        
      })
    })
  })

  describe("methods", () => {
  })
})