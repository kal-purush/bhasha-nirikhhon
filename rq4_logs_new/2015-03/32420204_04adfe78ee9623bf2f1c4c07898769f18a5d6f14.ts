//annee.spec.ts
//
/// <reference path='../../../src/typings/jasmine/jasmine.d.ts' />;
//
import Annee = require('../../../src/data/domain/annee');
//
describe('Annee Tests', () => {
  //
  var type:string = 'annee';
  var colname:string = 'annees';
  // empty constructor
  describe(" empty constructor", () => {
    var data: Annee;
    beforeEach(()=> {
      data = new Annee();
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
    it(" departementid property ", () => {
      expect(data.departementid).toBeDefined();
      expect(data.departementid).toBeNull();
      expect(data.has_departementid).toBeDefined();
      expect(data.has_departementid).toEqual(false);
    });
    it(" sigle property ", () => {
      expect(data.sigle).toBeDefined();
      expect(data.sigle).toBeNull();
      expect(data.has_sigle).toBeDefined();
      expect(data.has_sigle).toEqual(false);
    });
    it(" name property ", () => {
      expect(data.name).toBeDefined();
      expect(data.name).toBeNull();
      expect(data.has_name).toBeDefined();
      expect(data.has_name).toEqual(false);
    });
    it(" startDate property ", () => {
      expect(data.startDate).toBeDefined();
      expect(data.startDate).toBeNull();
      expect(data.has_startDate).toBeDefined();
      expect(data.has_startDate).toEqual(false);
    });
    it(" endDate property ", () => {
      expect(data.endDate).toBeDefined();
      expect(data.endDate).toBeNull();
      expect(data.has_endDate).toBeDefined();
      expect(data.has_endDate).toEqual(false);
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
      expect(oMap.type).toEqual(type);
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.avatarid).not.toBeDefined();
      expect(oMap.departementid).not.toBeDefined();
      expect(oMap.sigle).not.toBeDefined();
      expect(oMap.name).not.toBeDefined();
      expect(oMap.startDate).not.toBeDefined();
      expect(oMap.endDate).not.toBeDefined();
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap.type).toEqual(type);
      expect(oMap._id).not.toBeDefined();
      expect(oMap._rev).not.toBeDefined();
      expect(oMap.remarks).not.toBeDefined();
      expect(oMap.avatarid).not.toBeDefined();
      expect(oMap.departementid).not.toBeDefined();
      expect(oMap.sigle).not.toBeDefined();
      expect(oMap.name).not.toBeDefined();
      expect(oMap.startDate).not.toBeDefined();
      expect(oMap.endDate).not.toBeDefined();
    });
    //
  });
  // normal constructor
  describe(" normal constructor", () => {
    var data: Annee;
    //
    var id: any = 100;
    var rev: any = 3;
    var remarks: string = 'rem';
    var avatarid: any = 235;
    var departementid = 1000;
    var sigle: string = 'testsigle';
    var name: string = 'testname';
    var startDate: Date = new Date(1990, 10, 5);
    var endDate: Date = new Date(1991, 11, 6);
    //
    beforeEach(function() {
      var oMap: any = {
        _id: id, _rev: rev, remarks: remarks,
        avatarid: avatarid, departementid: departementid,
        sigle: sigle, name: name, startDate: startDate, endDate: endDate
      };
      data = new Annee(oMap);
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
    it(" departementid property ", () => {
      expect(data.departementid).toEqual(departementid);
      expect(data.has_departementid).toEqual(true);
    });
    it(" sigle property ", () => {
      expect(data.sigle).toEqual(sigle);
      expect(data.has_sigle).toEqual(true);
    });
    it(" name property ", () => {
      expect(data.name).toEqual(name);
      expect(data.has_name).toEqual(true);
    });
    it(" startDate property ", () => {
      expect(data.startDate).toEqual(startDate);
      expect(data.has_startDate).toEqual(true);
    });
    it(" endDate property ", () => {
      expect(data.endDate).toEqual(endDate);
      expect(data.has_endDate).toEqual(true);
    });
    it(" type property ", () => {
      expect(data.type).toEqual(type);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toEqual(colname);
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
      expect(oMap.avatarid).toEqual(avatarid);
      expect(oMap.departementid).toEqual(departementid);
      expect(oMap.sigle).toEqual(sigle);
      expect(oMap.name).toEqual(name);
      expect(data.startDate).toEqual(startDate);
      expect(data.endDate).toEqual(endDate);
    });
    it(" to_fetch_map ", () => {
      expect(data.to_fetch_map).toBeDefined();
      var oMap: any = {};
      data.to_fetch_map(oMap);
      expect(oMap.type).toEqual(type);
      expect(oMap._id).toEqual(id);
      expect(oMap._rev).toEqual(rev);
      expect(oMap.remarks).toEqual(remarks);
      expect(oMap.avatarid).toEqual(avatarid);
      expect(oMap.departementid).toEqual(departementid);
      expect(oMap.sigle).toEqual(sigle);
      expect(oMap.name).toEqual(name);
      expect(data.startDate).toEqual(startDate);
      expect(data.endDate).toEqual(endDate);
    });
    //
  });

});