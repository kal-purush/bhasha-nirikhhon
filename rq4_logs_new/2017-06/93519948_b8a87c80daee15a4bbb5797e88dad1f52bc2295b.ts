import { Bmod } from './bmod';
import { Project } from './project';
import { Requirement } from './requirement';

export class Task {
    id: number;
    name: string;
    type: string; // Enum {设计、开发、测试、研究、讨论、界面、事务、其他...}
    description?: string;
    /** 预估工时 */
    estimate?: number;
    /** 优先级 */
    priority?: number;
    /** 预计开始日期 */
    estimateStarted?: Date;
    /** 截止日期 */
    deadline?: Date;

    project: Project;
    bmod?: Bmod;
    assignedTo?: {id: number, name: string, displayName: string};
    requirement?: Requirement;

    // 影藏属性
    status?: string; // Enum {未开始、进行中、已完成、已关闭}



    // 工时信息
    // -- 预计开始日期 -- 截止日期 -- 预计工时
    /** 实际开始日期 */
    actsureStarted?: Date;
    /** 已耗时/总耗时 */
    usedTime?: number; // 单位小时
    leftTime?: number;

    // 任务周期 life circle
    createdBy?: {id: number, name: string, displayName: string};
    createdDate?: Date;
    finishedBy?: {id: number, name: string, displayName: string};
    caceledBy?: {id: number, name: string, displayName: string};
    closedBy?: {id: number, name: string, displayName: string};
    closeReason?: string;
    lastUpdatedBy?: {id: number, name: string, displayName: string};
    lastUpdatedDate?: Date;
}