//attacheddoc.spec.ts
//
/// <reference path='../../../src/typings/jasmine/jasmine.d.ts' />;
//
import AttachedDoc = require('../../../src/data/domain/attacheddoc');
//
describe('AttachedDoc Tests', () => {
    var type:string = 'attacheddoc';
    var colname:string = 'attacheddocs';
  // empty constructor
  describe(" empty constructor", () => {
    var data: AttachedDoc;
    beforeEach(() =>{
      data = new AttachedDoc();
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
    it(" mimetype property ", () => {
      expect(data.mimetype).toBeDefined();
      expect(data.mimetype).toBeNull();
      expect(data.has_mimetype).toBeDefined();
      expect(data.has_mimetype).toEqual(false);
    });
    it(" genre property ", () => {
      expect(data.genre).toBeDefined();
      expect(data.genre).toBeNull();
      expect(data.has_genre).toBeDefined();
      expect(data.has_genre).toEqual(false);
    });
    it(" data property ", () => {
      expect(data.data).toBeDefined();
      expect(data.data).toBeNull();
      expect(data.has_data).toBeDefined();
      expect(data.has_data).toEqual(false);
    });
    it(" name property ", () => {
      expect(data.name).toBeDefined();
      expect(data.name).toBeNull();
      expect(data.has_name).toBeDefined();
      expect(data.has_name).toEqual(false);
    });
    it(" type property ", () => {
      expect(data.type).toEqual(type);
      expect(data.has_type).toEqual(true);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toEqual(colname);
      expect(data.has_collection_name).toEqual(true);
    });
    it(" is_storeable property ", () => {
      expect(data.is_storeable).toBeDefined();
      expect(data.is_storeable).toEqual(false);
    });
    it(" to_insert_map ", () => {
      expect(data.to_insert_map).toBeDefined();
      var oMap: any = {};
      data.to_insert_map(oMap);
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.type).toEqual(type);
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.genre).not.toBeDefined();
      expect(oMap.mimetype).not.toBeDefined();
      expect(oMap.name).not.toBeDefined();
      expect(oMap.data).not.toBeDefined();
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.type).toEqual(type);
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.genre).not.toBeDefined();
      expect(oMap.mimetype).not.toBeDefined();
      expect(oMap.name).not.toBeDefined();
      expect(oMap.data).not.toBeDefined();
    });
    //
  });
  // normal constructor
  describe(" normal constructor", () => {
    var data: AttachedDoc;
    //
    var id:any = 100;
    var rev:any = 3;
    var remarks: string = 'rem';
    var name:string = 'testname';
    var genre:string = 'photo';
    var mimetype:string = 'image/jpeg';
    var xdata:any[] = [0,1,2,3,4,5];
    //
    beforeEach(() => {
      var oMap: any = { _id: id, _rev: rev, remarks: remarks,
      genre : genre, mimetype: mimetype,
      data: xdata, name:name };
      data = new AttachedDoc(oMap);
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
    it(" genre property ", () => {
      expect(data.genre).toEqual(genre);
      expect(data.has_genre).toEqual(true);
    });
    it(" mimetype property ", () => {
      expect(data.mimetype).toEqual(mimetype);
      expect(data.has_mimetype).toEqual(true);
    });
    it(" data property ", () => {
      expect(data.data.length).toBeGreaterThan(0);
      expect(data.has_data).toEqual(true);
    });
     it(" name property ", () => {
      expect(data.name).toEqual(name);
      expect(data.has_name).toEqual(true);
    });
    it(" type property ", () => {
      expect(data.type).toEqual(type);
      expect(data.has_type).toEqual(true);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toEqual(colname);
      expect(data.has_collection_name).toEqual(true);
    });
    it(" is_storeable property ", () => {
      expect(data.is_storeable).toEqual(true);
    });
    it(" to_insert_map ", () => {
      expect(data.to_insert_map).toBeDefined();
      var oMap: any = {};
      data.to_insert_map(oMap);
      expect(oMap.type).toEqual(type);
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.remarks).toEqual(remarks);
      expect(oMap.genre).toEqual(genre);
      expect(oMap.mimetype).toEqual(mimetype);
      expect(oMap.data.length).toBeGreaterThan(0);
      expect(oMap.name).toEqual(name);
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap.type).toEqual(type);
      expect(oMap._id).toEqual(id);
      expect(oMap._rev).toEqual(rev);
      expect(oMap.remarks).toEqual(remarks);
      expect(oMap.genre).toEqual(genre);
      expect(oMap.mimetype).toEqual(mimetype);
      expect(oMap.data.length).toBeGreaterThan(0);
      expect(oMap.name).toEqual(name);
    });
    //
  });

});