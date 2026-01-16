import fetch from 'node-fetch';
import moment from 'moment';
import { apikey, apisecret, users } from './config';
import DingAPI from './service/index';

interface User {
    name: string,
    phone: string | undefined
}

interface ITokenResponseProps {
    access_token: string
}

interface Department {
    dept_id: number,
    name: string,
    parent_id: number,
    create_dept_group: boolean,
    auto_add_user: boolean
}

async function main() {

    const token: ITokenResponseProps = await fetch(`https://oapi.dingtalk.com/gettoken?appkey=${apikey}&appsecret=${apisecret}`).then(res => res.json());
    const dingApi = new DingAPI(token.access_token);

    const startTime = moment().startOf('day').add('9', 'hours').format('YYYY-MM-DD HH:mm:ss');
    const endTime = moment().endOf('day').subtract('3', 'hours').format('YYYY-MM-DD HH:mm:ss');

    const bossID = 'manager9941';
    const quitUserIDs = ['565319034232652203'];

    // 获取所有的部门
    const allDeptsArray = await dingApi.getDepartments().then(res => {
        const parentIDs = res.map((dept: Department) => dept.dept_id);
        const promiseArray: any = [];
        parentIDs.forEach((deptID: number) => {
            const promise = dingApi.getChildrenDepartments(deptID);
            promiseArray.push(promise);
        })
        return Promise.all(promiseArray);
    }).catch((error) => {
        console.log(error);
        return [];
    });

    const allDepartments = allDeptsArray?.reduce((prev, cur) => {
        if (cur.length > 0) {
            return prev.concat(cur);
        }
        return prev;
    });

    const userIDfromDepartments: any[] = [];
    allDepartments.forEach((deptID: number) => {
        const promise = dingApi.getUserIDs(deptID);
        userIDfromDepartments.push(promise);
    })
    const allUserIDsArray = await Promise.all(userIDfromDepartments);
    // 所有员工ID(去掉老板id和已离职的员工)
    const allUserIDs = allUserIDsArray?.reduce((prev, cur) => {
        if (cur.length > 0) {
            return prev.concat(cur);
        }
        return prev;
    }).filter((userID: string) => userID !== bossID && !quitUserIDs.includes(userID));
    console.log('员工总计：' + allUserIDs.length + '人');

    // 当天应交日报的员工ID
    const attendance = await dingApi.getPunchIn(allUserIDs, startTime, endTime);
    const offDutyAttendance = attendance.filter((punch: any) => punch.checkType === 'OffDuty').map((item: any) => item.userId);

    const cursor = 0;
    let reports: any = [];

    const getAllReports = (cursor: number) => {
        return dingApi.getReport(startTime, endTime, cursor).then(async (report) => {
            reports = [...reports, ...report.data_list]
            if (report.has_more) {
                await getAllReports(report.next_cursor);
            }
            return [...reports, ...report.data_list]
        }).catch((error) => {
            console.log(error);
        });
    };
    await getAllReports(cursor);

    // 所有已交日报员工的ID(去除重复提交的日报)
    let allReportedUserIDs = reports.map((report: any) => report.creator_id);
    allReportedUserIDs = allReportedUserIDs.filter((id: string, index: number) => allReportedUserIDs.indexOf(id) === index);
    console.log('下班打卡：' + offDutyAttendance.length + '人');
    console.log('日志已提交：' + allReportedUserIDs.length + '人')

    // 未交日报员工
    let noReportUserIDs = offDutyAttendance.filter((item: string) => !allReportedUserIDs.includes(item));
    console.log(endTime + '未交日报员工：' + noReportUserIDs.length + '人');
    const promiseUserArray: any[] = [];
    noReportUserIDs.forEach((userID: string) => {
        const promise = dingApi.getUserDetail(userID);
        promiseUserArray.push(promise);
    })
    const allUserName = await Promise.all(promiseUserArray);
    console.log(allUserName.length === 0 ? (offDutyAttendance.length === 0 ? '今天无需提交日报' : '日报已交齐') : `${endTime}未交日报员工: ${allUserName}`);

    // 如果有未交日报员工的电话，并且不是周末
    if (allUserName.length > 0) {
        let phoneList: User[] = allUserName.map((user: string) => {
            return {
                'name': user,
                'phone': users.find(i => i.name === user)?.phone
            }
        });
        console.log(phoneList);
        phoneList.forEach((user: User) => {
            dingApi.sendSms(user, endTime);
        })
    }

    // const phoneNumber = await getUserPhoneNumber('vxO5Wt4hLb313t6wn192DwiEiE').then((user) => {
    //     console.log(user)
    // }).catch((err) => {
    //     console.log(err)
    // })

    // console.log(phoneNumber); 
}

main();