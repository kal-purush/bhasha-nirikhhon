import fetch from "node-fetch";
import config from "../config";
import { IAttendanceLeaveResult, IAttendanceListResult, IDepartmentIdResult, IDepartmentListsub, IDepartmentResult, IDingTalkUserListIdResult, IDingTalkUserResult, IReportSimpleListResult, IUserAttendanceResult } from "../interfaces";


export default class DingTalkApi {
    private apiUrl = config.dingTalk.apiUrl;
    private v1ApiUrl = config.dingTalk.v1ApiUrl;
    private v2ApiUrl = config.dingTalk.v2ApiUrl;
    private accessToken: string;
    constructor() {
        this.accessToken = global["DingTalkAccessToken"];
    }

    /**
     * 获取部门列表
     */
    async getDepartments(dept_id?: string) {
        const res = await fetch(`${this.v2ApiUrl}/department/listsub?access_token=${this.accessToken}`,
            { method: "POST" }).then((res): Promise<IDepartmentResult> => res.json());
        return res?.result;
    }

    /**
     * 获取子部门ID列表
     * dept_id 父部门ID 根部门传1
     */
    async getChildrenDepartments(dept_id: string) {
        const res = await fetch(
            `${this.v2ApiUrl}/department/listsubid?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify({
                    dept_id: dept_id,
                }),
            }
        ).then((res): Promise<IDepartmentIdResult> => res.json());
        return res?.result?.dept_id_list;
    };

    /**
     * 获取用户详情
     * @param userId
     */
    async getUserDetail(userId: string) {
        const res = await fetch(
            `${this.v2ApiUrl}/user/get?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify({
                    userid: userId,
                }),
            }
        ).then((res): Promise<IDingTalkUserResult> => res.json());
        return res.result;
    };

    /**
     * 获取部门用户userid列表
     * @param dept_id 
     */
    async getDepartmentUserIds(dept_id: number) {
        const res = await fetch(
            `${this.v1ApiUrl}/user/listid?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify({
                    dept_id
                }),
            }
        ).then((res): Promise<IDingTalkUserListIdResult> => res.json());
        return res.result?.userid_list;
    };

    /**
     * 获取打卡结果
     * @param userIdList 
     * @param startTime 
     * @param endTime 
     */
    async getAttendanceList(userIdList: number[], startTime: string, endTime: string) {
        const bodyData = {
            workDateFrom: startTime,
            offset: 0,
            userIdList: userIdList,
            limit: 50,
            isI18n: false,
            workDateTo: endTime,
        };
        const res = await fetch(
            `${this.apiUrl}/attendance/list?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify(bodyData)
            }
        ).then((res): Promise<IAttendanceListResult> => res.json());
        return res.recordresult;
    };

    /**
     * 获取用户发送日志的概要信息
     */
    async getReports(startTime: string, endTime: string, cursor: number, userId?: string) {
        const body = {
            cursor: cursor,
            start_time: new Date(startTime).getTime(),
            template_name: ["TIMESHEET", "日报"],
            size: 20,
            end_time: new Date(endTime).getTime(),
        };

        if (userId) {
            body["userid"] = userId;
        }

        const response = await fetch(
            `${this.v1ApiUrl}/report/simplelist?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify(body),
            }
        ).then((res): Promise<IReportSimpleListResult> => res.json());
        return response.result;
    };

    /**
     * 获取用户考勤数据
     */
    async getUserAttendance(userId: string, workdate: string) {
        const bodyData = {
            work_date: workdate,
            userid: userId
        };
        const res = await fetch(
            `${this.v1ApiUrl}/attendance/getupdatedata?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify(bodyData),
            }
        ).then((res): Promise<IUserAttendanceResult> => res.json());
        return res.result;
    }

    /**
     * 获取报表假期数据
     */
    async getUserAttendanceLeaveTimeByNames(userid: string, leave_names: string, from_date: string, to_date: string) {
        const bodyData = {
            userid: userid,
            leave_names: leave_names,
            from_date: from_date,
            to_date: to_date
        };
        const punchIn = await fetch(
            `${this.v1ApiUrl}/attendance/getleavetimebynames?access_token=${this.accessToken}`,
            {
                method: "POST",
                body: JSON.stringify(bodyData),
            }
        ).then((res): Promise<IAttendanceLeaveResult> => res.json());
        return punchIn.result;
    }

    
}