//departementperson.spec.ts
//
/// <reference path='../../../src/typings/jasmine/jasmine.d.ts' />;
//
import DepartementPerson = require('../../../src/data/domain/departementperson');
//
describe('DepartementPerson Tests', () => {
  // empty constructor
  describe(" empty constructor", () => {
    var data: DepartementPerson;
    beforeEach(() => {
      data = new DepartementPerson();
    });
    it(" id property ", () => {
      expect(data.id).toBeDefined();
      expect(data.id).toBeNull();
      expect(data.has_id).toBeDefined();
      expect(data.has_id).toEqual(false);
    });
    it(" rev property ", () => {
      expect(data.rev).toBeDefined();
      expect(data.rev).toBeNull();
      expect(data.has_rev).toBeDefined();
      expect(data.has_rev).toEqual(false);
    });
    it(" remarks property ", () => {
      expect(data.remarks).toBeDefined();
      expect(data.remarks).toBeNull();
      expect(data.has_remarks).toBeDefined();
      expect(data.has_remarks).toEqual(false);
    });
    it(" avatarid property ", () => {
      expect(data.avatarid).toBeDefined();
      expect(data.avatarid).toBeNull();
      expect(data.has_avatarid).toBeDefined();
      expect(data.has_avatarid).toEqual(false);
    });
    it(" personid property ", () => {
      expect(data.personid).toBeDefined();
      expect(data.personid).toBeNull();
      expect(data.has_personid).toBeDefined();
      expect(data.has_personid).toEqual(false);
    });
    it(" departementid property ", () => {
      expect(data.departementid).toBeDefined();
      expect(data.departementid).toBeNull();
      expect(data.has_departementid).toBeDefined();
      expect(data.has_departementid).toEqual(false);
    });
    it(" type property ", () => {
      expect(data.type).toBeDefined();
      expect(data.type).toBeNull();
      expect(data.has_type).toBeDefined();
      expect(data.has_type).toEqual(false);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toBeDefined();
      expect(data.collection_name).toBeNull();
      expect(data.has_collection_name).toBeDefined();
      expect(data.has_collection_name).toEqual(false);
    });
    it(" is_storeable property ", () => {
      expect(data.is_storeable).toBeDefined();
      expect(data.is_storeable).toEqual(false);
    });
    it(" to_insert_map ", () => {
      expect(data.to_insert_map).toBeDefined();
      var oMap: any = {};
      data.to_insert_map(oMap);
      expect(oMap.type).not.toBeDefined();
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.avatarid).not.toBeDefined();
      expect(oMap.personid).not.toBeDefined();
      expect(oMap.departementid).not.toBeDefined();
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap.type).not.toBeDefined();
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.avatarid).not.toBeDefined();
      expect(oMap.personid).not.toBeDefined();
      expect(oMap.departementid).not.toBeDefined();
    });
    //
  });
  // normal constructor
  describe(" empty constructor", () => {
    var data: DepartementPerson;
    var id: any = 100;
    var rev: any = 3;
    var remarks: string = 'rem';
    var avatarid: any = 235;
    var personid: any = 6000;
    var departementid: any = 500;
    //
    beforeEach(() => {
      var oMap: any = {
        _id: id, _rev: rev, remarks: remarks,
        avatarid: avatarid, personid: personid, departementid: departementid
      };
      data = new DepartementPerson(oMap);
    });
    it(" id property ", () => {
      expect(data.id).toEqual(id);
      expect(data.has_id).toEqual(true);
    });
    it(" rev property ", () => {
      expect(data.rev).toEqual(rev);
      expect(data.has_rev).toEqual(true);
    });
    it(" remarks property ", () => {
      expect(data.remarks).toEqual(remarks);
      expect(data.has_remarks).toEqual(true);
    });
    it(" avatarid property ", () => {
      expect(data.avatarid).toEqual(avatarid);
      expect(data.has_avatarid).toEqual(true);
    });
    it(" personid property ", () => {
      expect(data.personid).toEqual(personid);
      expect(data.has_personid).toEqual(true);
    });
    it(" departementid property ", () => {
      expect(data.departementid).toEqual(departementid);
      expect(data.has_departementid).toEqual(true);
    });
    it(" type property ", () => {
      expect(data.type).toBeDefined();
      expect(data.type).toBeNull();
      expect(data.has_type).toBeDefined();
      expect(data.has_type).toEqual(false);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toBeDefined();
      expect(data.collection_name).toBeNull();
      expect(data.has_collection_name).toBeDefined();
      expect(data.has_collection_name).toEqual(false);
    });
    it(" is_storeable property ", () => {
      expect(data.is_storeable).toBeDefined();
      expect(data.is_storeable).toEqual(false);
    });
    it(" to_insert_map ", () => {
      expect(data.to_insert_map).toBeDefined();
      var oMap: any = {};
      data.to_insert_map(oMap);
      expect(oMap.type).not.toBeDefined();
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.remarks).toEqual(remarks);
      expect(oMap.avatarid).toEqual(avatarid);
      expect(data.personid).toEqual(personid);
      expect(data.departementid).toEqual(departementid);
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap.type).not.toBeDefined();
      expect(oMap._id).toEqual(id);
      expect(oMap._rev).toEqual(rev);
      expect(oMap.remarks).toEqual(remarks);
      expect(oMap.avatarid).toEqual(avatarid);
      expect(data.personid).toEqual(personid);
      expect(data.departementid).toEqual(departementid);
    });
    //
  });

});