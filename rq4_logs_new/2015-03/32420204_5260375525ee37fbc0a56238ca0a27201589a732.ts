//profaffectation.spec.ts
//
/// <reference path='../../../src/typings/jasmine/jasmine.d.ts' />;
//
import ProfAffectation = require('../../../src/data/domain/profaffectation');
//
describe('ProfAffectation Tests', () => {
    var type:string = 'profaffectation';
    var colname:string = 'profaffectations';
  // empty constructor
  describe(" empty constructor", () => {
    var data: ProfAffectation;
    beforeEach(() => {
      data = new ProfAffectation();
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
    it(" anneeid property ", () => {
      expect(data.anneeid).toBeDefined();
      expect(data.anneeid).toBeNull();
      expect(data.has_anneeid).toBeDefined();
      expect(data.has_anneeid).toEqual(false);
    });
    it(" semestreid property ", () => {
      expect(data.semestreid).toBeDefined();
      expect(data.semestreid).toBeNull();
      expect(data.has_semestreid).toBeDefined();
      expect(data.has_semestreid).toEqual(false);
    });
    it(" groupeid property ", () => {
      expect(data.groupeid).toBeDefined();
      expect(data.groupeid).toBeNull();
      expect(data.has_groupeid).toBeDefined();
      expect(data.has_groupeid).toEqual(false);
    });
    it(" personid property ", () => {
      expect(data.personid).toBeDefined();
      expect(data.personid).toBeNull();
      expect(data.has_personid).toBeDefined();
      expect(data.has_personid).toEqual(false);
    });
    it(" enseignantid property ", () => {
      expect(data.enseignantid).toBeDefined();
      expect(data.enseignantid).toBeNull();
      expect(data.has_enseignantid).toBeDefined();
      expect(data.has_enseignantid).toEqual(false);
    });
    it(" uniteid property ", () => {
      expect(data.uniteid).toBeDefined();
      expect(data.uniteid).toBeNull();
      expect(data.has_uniteid).toBeDefined();
      expect(data.has_uniteid).toEqual(false);
    });
    it(" matiereid property ", () => {
      expect(data.matiereid).toBeDefined();
      expect(data.matiereid).toBeNull();
      expect(data.has_matiereid).toBeDefined();
      expect(data.has_matiereid).toEqual(false);
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
      expect(oMap.anneeid).not.toBeDefined();
      expect(oMap.semestreid).not.toBeDefined();
      expect(oMap.groupeid).not.toBeDefined();
      expect(oMap.startDate).not.toBeDefined();
      expect(oMap.end).not.toBeDefined();
      expect(oMap.enseignantid).not.toBeDefined();
      expect(oMap.uniteid).not.toBeDefined();
      expect(oMap.matiereid).not.toBeDefined();
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
      expect(oMap.anneeid).not.toBeDefined();
      expect(oMap.semestreid).not.toBeDefined();
      expect(oMap.groupeid).not.toBeDefined();
      expect(oMap.startDate).not.toBeDefined();
      expect(oMap.end).not.toBeDefined();
      expect(oMap.enseignantid).not.toBeDefined();
      expect(oMap.uniteid).not.toBeDefined();
      expect(oMap.matiereid).not.toBeDefined();
    });
    //
  });
  // normal constructor
  describe(" normal constructor", () => {
    var data: ProfAffectation;
    var id:any = 100;
    var rev:any = 3;
    var remarks: string = 'rem';
    var avatarid: any = 235;
    var departementid:any = 1000;
    var anneeid:any = 2000;
    var semestreid: any = 3000;
    var personid: any = 4000;
    var groupeid: any=5000;
    var startDate: Date = new Date(2014,9,3);
    var endDate: Date = new Date(2015,8,10);
    var enseignantid:any = 6000;
    var uniteid:any = 3000;
    var matiereid: any = 1500;
    //
    beforeEach(() => {
      var oMap: any = { _id: id, _rev: rev, remarks: remarks,
      avatarid : avatarid,
      departementid: departementid, anneeid:anneeid,semestreid:semestreid,
          groupeid:groupeid, personid:personid,
          startDate: startDate, endDate: endDate,
          enseignantid: enseignantid, uniteid:uniteid, matiereid:matiereid
      };
      data = new ProfAffectation(oMap);
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
    it(" anneeid property ", () => {
      expect(data.anneeid).toEqual(anneeid);
      expect(data.has_anneeid).toEqual(true);
    });
    it(" semestreid property ", () => {
      expect(data.semestreid).toEqual(semestreid);
      expect(data.has_semestreid).toEqual(true);
    });
    it(" groupeid property ", () => {
      expect(data.groupeid).toEqual(groupeid);
      expect(data.has_groupeid).toEqual(true);
    });
    it(" personid property ", () => {
      expect(data.personid).toEqual(personid);
      expect(data.has_personid).toEqual(true);
    });
    it(" enseignantid property ", () => {
      expect(data.enseignantid).toEqual(enseignantid);
      expect(data.has_enseignantid).toEqual(true);
    });
    it(" uniteid property ", () => {
      expect(data.uniteid).toEqual(uniteid);
      expect(data.has_uniteid).toEqual(true);
    });
    it(" matiereid property ", () => {
      expect(data.matiereid).toEqual(matiereid);
      expect(data.has_matiereid).toEqual(true);
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
      expect(data.has_type).toEqual(true);
    });
    it(" collection_name property ", () => {
      expect(data.collection_name).toEqual(colname);
      expect(data.has_collection_name).toEqual(true);
    });
    it(" is_storeable property ", () => {
      expect(data.is_storeable).toBeDefined();
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
      expect(oMap.anneeid).toEqual(anneeid);
      expect(oMap.semestreid).toEqual(semestreid);
      expect(oMap.groupeid).toEqual(groupeid);
      expect(oMap.startDate).toEqual(startDate);
      expect(oMap.endDate).toEqual(endDate);
      expect(oMap.enseignantid).toEqual(enseignantid);
      expect(oMap.uniteid).toEqual(uniteid);
      expect(oMap.matiereid).toEqual(matiereid);
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
      expect(oMap.anneeid).toEqual(anneeid);
      expect(oMap.semestreid).toEqual(semestreid);
      expect(oMap.groupeid).toEqual(groupeid);
      expect(oMap.startDate).toEqual(startDate);
      expect(oMap.endDate).toEqual(endDate);
      expect(oMap.enseignantid).toEqual(enseignantid);
      expect(oMap.uniteid).toEqual(uniteid);
      expect(oMap.matiereid).toEqual(matiereid);
    });
    //
  });

});