"use server";
import redis from "@/lib/redis";
const fs = require("fs");
import _ from "lodash"

export async function readReddis(key) {
  return await redis.call("JSON.GET", key);
}

export async function writeReddis(key, json) {
  return await redis.call("JSON.SET", key, "$", JSON.stringify(json));
}

export async function createAppGroup(appGroupObj, tenant) {
  try {
    const obj = JSON.parse(appGroupObj);
    const newAppGroup = obj[tenant].AG;
    const mainJSON = await readReddis("tenantJson");
    if (mainJSON) {
      var completeData = JSON.parse(mainJSON);
      if(completeData.hasOwnProperty("TENANT") && Array.isArray(completeData['TENANT'])){
        const index = completeData['TENANT'].findIndex((item) => item.Code == tenant)
        completeData["TENANT"][index]["AG"].push(newAppGroup);
        await writeReddis("tenantJson" , completeData)
        return {data: newAppGroup.code};
      }
      
    } else {
      return {error :"Error occured"};
    }
  } catch (err) {
    return {error :"Error occured"};
  }
}

export async function createApp(appObj, tenant , group) {
  try {
    const obj = JSON.parse(appObj);
    const newApp = obj[group].APP;    
    const mainJSON = await readReddis("tenantJson");
    if (mainJSON) {
      var completeData = JSON.parse(mainJSON);
      if(completeData.hasOwnProperty("TENANT") && Array.isArray(completeData['TENANT'])){
        const tenantIndex = completeData['TENANT'].findIndex((item) => item.Code == tenant)
        const groupIndex = completeData["TENANT"][tenantIndex]["AG"].findIndex((item)=>item.code == group)
        completeData["TENANT"][tenantIndex]["AG"][groupIndex]["APPS"].push(newApp);
        await writeReddis("tenantJson" , completeData)
        return {data: newApp.code};
      }
      
    } else {
      return {error :"Error occured"};
    }
  } catch (err) {
    return {error :"Error occured"};
  }
}