import "reflect-metadata";
import 'mocha';
import * as should from 'should'
import {Maintain} from '../src/models/Maintain';
import {MockSheetService} from './mock';
import {MaintainService} from "../src/services/MaintainService/MaintainService";

describe('測試MaintainService', function () {
    it('測試querySheet成功取得Maintain資料', async function () {
        const maintainService = new MaintainService(new MockSheetService());
        const result: Maintain = await maintainService.getMaintainById("1");
        should(result.Id).be.equal("TestMaintainId");
    })
});

describe('測試MaintainService', function () {
    it('測試getMaintainState尚未完成', async function () {
        const maintainService = new MaintainService(new MockSheetService());
        const state = maintainService.getMaintainState("0");
        should(state).be.equal("尚未完成");
    })
});

describe('測試MaintainService', function () {
    it('測試getMaintainState已在維修中', async function () {
        const maintainService = new MaintainService(new MockSheetService());
        const state = maintainService.getMaintainState("1");
        should(state).be.equal("已在維修中，請耐心等候");
    })
});

describe('測試MaintainService', function () {
    it('測試getMaintainState已經完成', async function () {
        const maintainService = new MaintainService(new MockSheetService());
        const state = maintainService.getMaintainState("2");
        should(state).be.equal("已經完成囉，感謝您的報修");
    })
});

describe('測試MaintainService', function () {
    it('測試requestReport取得message', async function () {
        const maintainService = new MaintainService(new MockSheetService());
        const message = maintainService.requestReport("unittest123") as any;

        should(message.template.actions[0].uri).be.equal("https://docs.google.com/forms/d/e/1FAIpQLSd_xr_18k4FIPjBYECcwv2fc1dOT_IuZMxAgGJUuseg9KInmw/viewform?usp=pp_url&entry.815484785&entry.534784453&entry.1173029400&entry.142495844&entry.1574186958&entry.780437475=unittest123");
    })
});