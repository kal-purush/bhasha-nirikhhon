"use strict";

const Iota=require('iota')();
const cloneDeep = require('lodash/cloneDeep');
const MongoModels=require('mongo-models');
const showShallowDiff = require('./showShallowDiff');


// Iota uses logger
const log4js=require('log4js');

if(!global.logger) {
  global.logger=log4js.getLogger("node");
  log4js.configure({
    appenders: { err: { type: 'stderr' } },
    categories: { default: { appenders: ['err'], level: 'DEBUG' } }
  });
}

var encivInfo={_ids: {}}

function encivQuery(query,options) {
    return new Promise(async (ok, ko) => {
        try {
            var data=await Iota.find(query,options);
            if (!data || !data.length) return ok([])
            return ok(data);
        }
        catch (error) {
            console.error("encivQuery caught error", error);
            return ko(error);
        }
    })
}


function encivQueryAll(query) {
    var options={skip: 0, limit: 1000}
    var data = [];
    return new Promise(async (ok, ko) => {
        while (1) {
            try {
                var newData = await encivQuery(query,options);
                data = data.concat(newData);
                if (newData.length < options.limit) {
                    return ok(data);
                }
                options.skip+=options.limit;
            }
            catch (error) {
                console.error("encivQueryAll caught error", error);
                return ko(error)
            }
        }
    })
}

function createOneInTable(iota,table){
    return new Promise(async (ok,ko)=>{
        try {
            var doc=await Iota.create(iota);
            encivInfo[table][doc._id]=doc;
            encivInfo._ids[table].push(doc._id.toString()); // object properties are always strings
            return ok(doc);
        }
        catch(err){
            console.error("create viewer caught error:", err);
            return ko(err);
        }
    })
}
encivInfo.createOneInTable=createOneInTable;

function updateOne(query,update){
    if(query._id && typeof query._id==="string") query._id=Iota.ObjectID(query._id);
    return new Promise(async (ok,ko)=>{
        try {
            var result=await Iota.updateOne(query,update);
            if(!(result.matchedCount===1 && result.modifiedCount===1))
                console.error("encivInfo.updateOne did nothing", query, update)
            return ok();
        }
        catch(err){
            console.error("encivInfo updateOne caught error", err);
            return ko(err);
        }
    })
}

encivInfo.updateOne=updateOne;

function replaceOne(query,update){
    if(query._id && typeof query._id==="string") query._id=Iota.ObjectID(query._id);
    return new Promise(async (ok,ko)=>{
        try {
            var result=await Iota.replaceOne(query,update);
            if(!(result && result.length===1))
                console.error("encivInfo.replaceOne returned", result, query, update)
            return ok();
        }
        catch(err){
            console.error("encivInfo replaceOne caught error", err);
            return ko(err);
        }
    })
}

encivInfo.replaceOne=replaceOne;

function updateOrCreateIota(query,overWriter){
    return new Promise(async (ok,ko)=>{
        var viewers=await Iota.find(query);
        if(viewers.length==0){ // create the new race
            var newViewer=cloneDeep(viewer);
            overWriter(newViewer);
            try {
                var viewerObj=await Iota.create(newViewer);
                return ;
            }
            catch(err){
                console.error("create viewer caught error:", err);
                ko(err);
            }
            ok(newViewer);
        }else if(viewers.length){ // update the race
            if(viewers.length>1) console.error("found multiple viewers with the same path, updating the first one", viewers);
            var viewerObj=cloneDeep(viewers[0]);
            mergeWithVerbose(viewerObj,viewer);
            overWriteViewerInfo(viewerObj)
            await Iota.findOneAndReplace({_id: viewerObj._id},viewerObj)
            ok(viewerObj)
        }
    })
}


const maxInList=10000; // the max in a mongo query is really limited by the size of the entire query doc, but that's 16M
function into_Db_add_Table_of_docs_matching_Query_with_List_segments_applied_by_InListifer(db,table, query, list, inListifier) { // inListifier: (q,list)=>q.bp_stage=list but list and inListifier might be undefined
    let begin = 0;
    return new Promise(async (ok, ko) => {
        try {
            while (true) {
                var q = cloneDeep(query);
                var inList=list && list.slice(begin,begin+maxInList) || [];
                inListifier && inListifier(q, inList);
                var docs = await encivQueryAll(q);
                table = encivTableify(db, table, docs);
                if((begin+=maxInList)>(list && list.length || 0))
                    return ok(table);
            }
        }
        catch (err) {
            console.error("gatherAll caught error", err);
            ko(err);
        }
    })
}

function encivTableify(db,table, docs) {
    if(!db[table]) db[table]={};
    docs.forEach(doc => {
        if (!db[table][doc._id]) {
            db[table][doc._id] = doc;
        } else {
            if(!showShallowDiff(db[table][doc._id], doc))
                console.info("encivTableify _id already present no differences found",doc._id)
        }
    })
    db._ids[table]=Object.keys(db[table]);
    return table;
}

function init(YYYY_MM_DD="2020-01-01"){
    return new Promise(async (ok,ko)=>{
        try{
            await Iota.connectInit();
            await into_Db_add_Table_of_docs_matching_Query_with_List_segments_applied_by_InListifer(encivInfo,'viewers',{"bp_info.stage_id": {$exists: true}, "bp_info.election_date": {$gte: YYYY_MM_DD}});
            await into_Db_add_Table_of_docs_matching_Query_with_List_segments_applied_by_InListifer(encivInfo,'recorders',{"bp_info.candidate_stage_result_id": {$exists: true}, "bp_info.election_date": {$gte: YYYY_MM_DD}});
            await into_Db_add_Table_of_docs_matching_Query_with_List_segments_applied_by_InListifer(encivInfo,'participants',{"component.participant.bp_info.election_date": {$gte: YYYY_MM_DD}});
            return ok();
        }
        catch(err){
            ko(err);
        }
    })
}

function disconnect(){
    MongoModels.disconnect();
}


encivInfo.init=init;
encivInfo.disconnect=disconnect;

module.exports=encivInfo;