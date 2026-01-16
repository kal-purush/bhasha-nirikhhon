
/*
 * -------------------------------------------------------
 * THIS FILE WAS AUTOMATICALLY GENERATED (DO NOT MODIFY)
 * -------------------------------------------------------
 */

/* tslint:disable */
/* eslint-disable */

export class AddMemberToScheduleRequestInput {
    userId: number;
    employeeIds: number[];
    scheduleId: number;
}

export class AddNewEmployeeRequestInput {
    email: string;
    positionId: number;
    roleId: number;
    scheduleId?: Nullable<number>;
    firstName: string;
    middleName?: Nullable<string>;
    lastName: string;
}

export class ApproveChangeShiftRequestInput {
    userId: number;
    notificationId: number;
    isApproved: boolean;
}

export class ApproveESLChangeShiftRequestInput {
    teamLeaderId: number;
    notificationId: number;
    isApproved: boolean;
}

export class ApproveLeaveUndertimeRequestInput {
    userId: number;
    notificationId: number;
    isApproved: boolean;
}

export class ApproveOvertimeRequestInput {
    userId: number;
    overtimeId?: Nullable<number>;
    notificationId?: Nullable<number>;
    approvedMinutes?: Nullable<number>;
    isApproved: boolean;
    managerRemarks?: Nullable<string>;
}

export class ApproveOvertimeSummaryRequestInput {
    approveOvertimeRequests: ApproveOvertimeRequestInput[];
}

export class CancelLeaveRequestInput {
    userId: number;
    leaveId: number;
}

export class ChangeSchedRequestInput {
    leaderIds: number[];
    workingDays: WorkingDayTimesRequestInput[];
}

export class CreateBulkOvertimeRequestInput {
    managerId: number;
    otherProject?: Nullable<string>;
    requestedMinutes: number;
    remarks: string;
    date: string;
    employeeIds: number[];
    projectId: number;
}

export class CreateChangeShiftRequestInput {
    userId: number;
    timeEntryId: number;
    managerId: number;
    timeIn: string;
    timeOut: string;
    otherProject?: Nullable<string>;
    description: string;
    projects: MultiProjectRequestInput[];
}

export class CreateESLChangeShiftRequestInput {
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    timeIn: string;
    timeOut: string;
    description: string;
    eslOffsetIDs: number[];
}

export class CreateESLOffsetRequestInput {
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    timeIn: string;
    timeOut: string;
    title: string;
    description: string;
}

export class CreateEmployeeScheduleRequestInput {
    userId: number;
    scheduleName?: Nullable<string>;
    workingDays: WorkingDayTimesRequestInput[];
}

export class CreateInterruptionRequestInput {
    timeEntryId: number;
    workInterruptionTypeId: number;
    timeOut?: Nullable<string>;
    timeIn?: Nullable<string>;
    remarks?: Nullable<string>;
    otherReason?: Nullable<string>;
}

export class CreateLeaveRequestInput {
    userId: number;
    leaveTypeId: number;
    managerId: number;
    otherProject?: Nullable<string>;
    reason?: Nullable<string>;
    leaveProjects: MultiProjectRequestInput[];
    leaveDates: LeaveDateRequestInput[];
}

export class CreateOvertimeRequestInput {
    userId: number;
    managerId: number;
    timeEntryId: number;
    otherProject?: Nullable<string>;
    requestedMinutes: number;
    remarks?: Nullable<string>;
    date: string;
    overtimeProjects: MultiProjectRequestInput[];
}

export class CreateSummaryRequestInput {
    startDate: string;
    endDate: string;
}

export class DeleteEmployeeScheduleRequestInput {
    userId: number;
    employeeScheduleId: number;
}

export class LeaveDateRequestInput {
    leaveDate: string;
    isWithPay: boolean;
    days: number;
}

export class LogoutRequestInput {
    token: string;
}

export class MultiProjectRequestInput {
    projectId: number;
    projectLeaderId: number;
}

export class NotificationRequestInput {
    id: number;
    readAt?: Nullable<DateTime>;
}

export class SearchEmployeesByScheduleRequestInput {
    employeeScheduleId: number;
    searchKey: string;
}

export class ShowInterruptionRequestInput {
    timeEntryId: number;
}

export class TimeInRequestInput {
    id: number;
    userId: number;
    timeHour: TimeSpan;
    date: DateTime;
    startTime: TimeSpan;
    endTime: TimeSpan;
    remarks?: Nullable<string>;
    files?: Nullable<Upload[]>;
}

export class TimeOutRequestInput {
    userId: number;
    timeEntryId?: Nullable<number>;
    timeHour: TimeSpan;
    remarks?: Nullable<string>;
    workedHours?: Nullable<string>;
}

export class UpdateEmployeeScheduleRequestInput {
    userId: number;
    employeeScheduleId: number;
    scheduleName?: Nullable<string>;
    workingDays: WorkingDayTimesRequestInput[];
}

export class UpdateInterruptionRequestInput {
    id: number;
    workInterruptionTypeId: number;
    timeOut?: Nullable<string>;
    timeIn?: Nullable<string>;
    remarks?: Nullable<string>;
    otherReason?: Nullable<string>;
}

export class UpdateLeaveRequestInput {
    userId: number;
    leaveTypeId: number;
    leaveId: number;
    managerId: number;
    otherProject?: Nullable<string>;
    reason?: Nullable<string>;
    leaveProjects: MultiProjectRequestInput[];
    leaveDates: LeaveDateRequestInput[];
}

export class UpdateMemberScheduleRequestInput {
    userId: number;
    employeeId: number;
    scheduleId: number;
}

export class UpdateTimeEntryInput {
    userId: number;
    timeEntryId: number;
    timeIn?: Nullable<string>;
    timeOut?: Nullable<string>;
}

export class WorkingDayTimesRequestInput {
    day?: Nullable<string>;
    from?: Nullable<string>;
    to?: Nullable<string>;
    breakFrom?: Nullable<string>;
    breakTo?: Nullable<string>;
}

export class ChangeScheduleRequest {
    id: number;
    userId: number;
    isManagerApproved?: Nullable<boolean>;
    isLeaderApproved?: Nullable<boolean>;
    data: string;
    user: User;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ChangeShiftDTO {
    timeIn: string;
    timeOut: string;
    id: number;
    userId: number;
    timeEntryId: number;
    managerId: number;
    otherProject?: Nullable<string>;
    description: string;
    isManagerApproved?: Nullable<boolean>;
    isLeaderApproved?: Nullable<boolean>;
    user: User;
    manager: User;
    timeEntry: TimeEntry;
    multiProjects: MultiProject[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ChangeShiftNotification {
    changeShiftRequestId: number;
    changeShiftRequest: ChangeShiftRequest;
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ChangeShiftRequest {
    id: number;
    userId: number;
    timeEntryId: number;
    managerId: number;
    timeIn: TimeSpan;
    timeOut: TimeSpan;
    otherProject?: Nullable<string>;
    description: string;
    isManagerApproved?: Nullable<boolean>;
    isLeaderApproved?: Nullable<boolean>;
    user: User;
    manager: User;
    timeEntry: TimeEntry;
    multiProjects: MultiProject[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class Day {
    isDaySelected?: Nullable<boolean>;
    workingDay?: Nullable<string>;
    timeIn?: Nullable<string>;
    timeOut?: Nullable<string>;
    breakFrom?: Nullable<string>;
    breakTo?: Nullable<string>;
}

export class ESLChangeShiftDTO {
    timeIn: string;
    timeOut: string;
    id: number;
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    description: string;
    isLeaderApproved?: Nullable<boolean>;
    user: User;
    teamLeader: User;
    timeEntry: TimeEntry;
    eslOffsets: ESLOffset[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ESLChangeShiftNotification {
    eslChangeShiftRequestId: number;
    eslChangeShiftRequest: ESLChangeShiftRequest;
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ESLChangeShiftRequest {
    id: number;
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    timeIn: TimeSpan;
    timeOut: TimeSpan;
    description: string;
    isLeaderApproved?: Nullable<boolean>;
    user: User;
    teamLeader: User;
    timeEntry: TimeEntry;
    eslOffsets: ESLOffset[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ESLOffset {
    id: number;
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    eslChangeShiftRequestId?: Nullable<number>;
    timeIn: TimeSpan;
    timeOut: TimeSpan;
    title: string;
    description: string;
    isLeaderApproved?: Nullable<boolean>;
    isUsed: boolean;
    user: User;
    teamLeader: User;
    timeEntry: TimeEntry;
    eslChangeShiftRequest: ESLChangeShiftRequest;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ESLOffsetDTO {
    timeIn: string;
    timeOut: string;
    id: number;
    userId: number;
    timeEntryId: number;
    teamLeaderId: number;
    eslChangeShiftRequestId?: Nullable<number>;
    title: string;
    description: string;
    isLeaderApproved?: Nullable<boolean>;
    isUsed: boolean;
    user: User;
    teamLeader: User;
    timeEntry: TimeEntry;
    eslChangeShiftRequest: ESLChangeShiftRequest;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class ESLOffsetNotification {
    eslOffsetId: number;
    eslOffset: ESLOffset;
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class EmployeeSchedule {
    id: number;
    name?: Nullable<string>;
    users: User[];
    workingDayTimes: WorkingDayTime[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class EmployeeScheduleDTO {
    id?: Nullable<number>;
    scheduleName?: Nullable<string>;
    days: Day[];
    memberCount: number;
    employeeScheduleId: number;
    day?: Nullable<string>;
    from: TimeSpan;
    to: TimeSpan;
    breakFrom: TimeSpan;
    breakTo: TimeSpan;
    employeeSchedule: EmployeeSchedule;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class File {
    mimeType: string;
    link: string;
    fileName: string;
}

export class HeatMapDTO {
    leaveValue?: number;
    day: number;
    value: number;
    leaveName?: Nullable<string>;
}

export class Leave {
    id: number;
    userId: number;
    leaveTypeId: number;
    managerId?: Nullable<number>;
    otherProject?: Nullable<string>;
    reason?: Nullable<string>;
    leaveDate: DateTime;
    days: number;
    isWithPay: boolean;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    isDeleted: boolean;
    leaveProjects: MultiProject[];
    leaveType: LeaveType;
    manager: User;
    user: User;
    leaveNotifications: LeaveNotification[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class LeaveBreakdownDTO {
    sickLeave: number;
    undertime: number;
    vacationLeave: number;
    emergencyLeave: number;
    bereavementLeave: number;
    maternityLeave: number;
    withoutPayTotal: number;
    withPayTotal: number;
    pending: number;
}

export class LeaveDTO {
    userId?: Nullable<number>;
    avatar?: Nullable<string>;
    userName?: Nullable<string>;
    userRole?: Nullable<string>;
    leaveType?: Nullable<string>;
    manager?: Nullable<string>;
    reason?: Nullable<string>;
    leaveDate?: Nullable<DateTime>;
    isWithPay?: Nullable<boolean>;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    days?: Nullable<number>;
    createdAt?: Nullable<DateTime>;
    id: number;
    leaveTypeId: number;
    managerId?: Nullable<number>;
    otherProject?: Nullable<string>;
    isDeleted: boolean;
    leaveProjects: MultiProject[];
    user: User;
    leaveNotifications: LeaveNotification[];
    updatedAt?: Nullable<DateTime>;
}

export class LeaveHeatMapDTO {
    january: HeatMapDTO[];
    february: HeatMapDTO[];
    march: HeatMapDTO[];
    april: HeatMapDTO[];
    may: HeatMapDTO[];
    june: HeatMapDTO[];
    july: HeatMapDTO[];
    august: HeatMapDTO[];
    september: HeatMapDTO[];
    october: HeatMapDTO[];
    november: HeatMapDTO[];
    december: HeatMapDTO[];
}

export class LeaveNotification {
    leaveId: number;
    leave: Leave;
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class LeaveType {
    id: number;
    name: string;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class LeavesDTO {
    heatmap: LeaveHeatMapDTO;
    table: LeavesTableDTO[];
    breakdown: LeaveBreakdownDTO;
    user?: Nullable<User>;
    totalNumberOfFiledLeaves?: Nullable<number>;
}

export class LeavesTableDTO {
    date?: Nullable<DateTime>;
    createdAt?: Nullable<DateTime>;
    leaveTypeId: number;
    isWithPay: boolean;
    reason?: Nullable<string>;
    status?: Nullable<string>;
    numLeaves: number;
    userName?: Nullable<string>;
    leaveName?: Nullable<string>;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    leaveId: number;
    userId: number;
}

export class Media {
    id: number;
    collectionName?: Nullable<string>;
    name?: Nullable<string>;
    fileName?: Nullable<string>;
    mimeType?: Nullable<string>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class MultiProject {
    id: number;
    type: string;
    projectId?: Nullable<number>;
    projectLeaderId?: Nullable<number>;
    leaveId?: Nullable<number>;
    overtimeId?: Nullable<number>;
    changeShiftRequestId?: Nullable<number>;
    project: Project;
    projectLeader: User;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export abstract class IMutation {
    abstract updateTimeIn(timeIn: TimeInRequestInput): string | Promise<string>;

    abstract updateTimeOut(timeOut: TimeOutRequestInput): string | Promise<string>;

    abstract createSignIn(): string | Promise<string>;

    abstract logout(logOut: LogoutRequestInput): string | Promise<string>;

    abstract createWorkInterruption(interruption: CreateInterruptionRequestInput): WorkInterruptionDTO | Promise<WorkInterruptionDTO>;

    abstract updateWorkInterruption(interruption: UpdateInterruptionRequestInput): boolean | Promise<boolean>;

    abstract deleteWorkInterruption(id: number): boolean | Promise<boolean>;

    abstract updateOneTimeEntry(updatedTimeEntry: UpdateTimeEntryInput): string | Promise<string>;

    abstract createLeave(leave: CreateLeaveRequestInput): Leave[] | Promise<Leave[]>;

    abstract updateLeave(leave: UpdateLeaveRequestInput): Nullable<string> | Promise<Nullable<string>>;

    abstract cancelLeave(request: CancelLeaveRequestInput): string | Promise<string>;

    abstract readNotification(notification: NotificationRequestInput): string | Promise<string>;

    abstract isReadAll(id: number): Notification[] | Promise<Notification[]>;

    abstract createOvertime(overtime: CreateOvertimeRequestInput): Overtime | Promise<Overtime>;

    abstract createSummarizedOvertime(overtimeSummary: CreateSummaryRequestInput): string | Promise<string>;

    abstract createBulkOvertime(request: CreateBulkOvertimeRequestInput): Overtime[] | Promise<Overtime[]>;

    abstract approveDisapproveOvertime(approvingData: ApproveOvertimeRequestInput): boolean | Promise<boolean>;

    abstract approveDisapproveAllOvertimeSummary(approvingDatas: ApproveOvertimeSummaryRequestInput): string | Promise<string>;

    abstract approveDisapproveLeave(approvingData: ApproveLeaveUndertimeRequestInput): boolean | Promise<boolean>;

    abstract approveDisapproveUndertime(approvingData: ApproveLeaveUndertimeRequestInput): boolean | Promise<boolean>;

    abstract approveDisapproveChangeShift(approvingData: ApproveChangeShiftRequestInput): boolean | Promise<boolean>;

    abstract createChangeShift(request: CreateChangeShiftRequestInput): ChangeShiftRequest | Promise<ChangeShiftRequest>;

    abstract createESLChangeShift(request: CreateESLChangeShiftRequestInput): ESLChangeShiftRequest | Promise<ESLChangeShiftRequest>;

    abstract approveDisapproveESLChangeShiftStatus(request: ApproveESLChangeShiftRequestInput): ESLChangeShiftRequest | Promise<ESLChangeShiftRequest>;

    abstract createESLOffset(request: CreateESLOffsetRequestInput): ESLOffset | Promise<ESLOffset>;

    abstract approveDisapproveChangeOffsetStatus(request: ApproveESLChangeShiftRequestInput): ESLOffset | Promise<ESLOffset>;

    abstract createEmployeeSchedule(request: CreateEmployeeScheduleRequestInput): string | Promise<string>;

    abstract updateEmployeeSchedule(request: UpdateEmployeeScheduleRequestInput): string | Promise<string>;

    abstract addMembersToSchedule(request: AddMemberToScheduleRequestInput): string | Promise<string>;

    abstract updateMemberSchedule(request: UpdateMemberScheduleRequestInput): string | Promise<string>;

    abstract deleteEmployeeSchedule(request: DeleteEmployeeScheduleRequestInput): string | Promise<string>;

    abstract changeScheduleRequest(request: ChangeSchedRequestInput): ChangeScheduleRequest | Promise<ChangeScheduleRequest>;

    abstract addNewEmployee(request: AddNewEmployeeRequestInput): boolean | Promise<boolean>;
}

export class MyOvertimeDTO {
    id: number;
    projects: MultiProject[];
    otherProject?: Nullable<string>;
    supervisor: string;
    dateFiled?: Nullable<DateTime>;
    remarks: string;
    overtimeDate?: Nullable<DateTime>;
    requestedMinutes?: Nullable<number>;
    approvedMinutes?: Nullable<number>;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    userId: number;
    managerId?: Nullable<number>;
    timeEntryId: number;
    managerRemarks?: Nullable<string>;
    multiProjects: MultiProject[];
    manager: User;
    user: User;
    timeEntry: TimeEntry;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class Notification {
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class Over {
    id: number;
    link: string;
    name: string;
    roleId: number;
    roleName: string;
}

export class Overtime {
    id: number;
    userId: number;
    managerId?: Nullable<number>;
    timeEntryId: number;
    otherProject?: Nullable<string>;
    remarks?: Nullable<string>;
    overtimeDate: DateTime;
    requestedMinutes: number;
    approvedMinutes?: Nullable<number>;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    managerRemarks?: Nullable<string>;
    multiProjects: MultiProject[];
    manager: User;
    user: User;
    timeEntry: TimeEntry;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class OvertimeDTO {
    user?: Nullable<Over>;
    id: number;
    projects: MultiProject[];
    otherProject?: Nullable<string>;
    supervisor: string;
    dateFiled?: Nullable<DateTime>;
    remarks: string;
    overtimeDate?: Nullable<DateTime>;
    approvedMinutes?: Nullable<number>;
    isLeaderApproved?: Nullable<boolean>;
    isManagerApproved?: Nullable<boolean>;
    managerRemarks?: Nullable<string>;
    userId: number;
    managerId?: Nullable<number>;
    timeEntryId: number;
    requestedMinutes: number;
    multiProjects: MultiProject[];
    manager: User;
    timeEntry: TimeEntry;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class OvertimeNotification {
    overtimeId: number;
    overtime: Overtime;
    id: number;
    recipientId?: Nullable<number>;
    relatedEntityId?: Nullable<number>;
    type: string;
    data: string;
    readAt?: Nullable<DateTime>;
    isRead: boolean;
    recipient?: Nullable<User>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class Position {
    id: number;
    name: string;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class Project {
    id: number;
    projectLeaderId?: Nullable<number>;
    projectSubLeaderId?: Nullable<number>;
    name: string;
    projectLeader?: Nullable<User>;
    projectSubLeader?: Nullable<User>;
    leaves: Leave[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export abstract class IQuery {
    abstract getHello(): string | Promise<string>;

    abstract userById(): Nullable<UserDTO> | Promise<Nullable<UserDTO>>;

    abstract userByEmail(email: string): Nullable<UserDTO> | Promise<Nullable<UserDTO>>;

    abstract allUsers(): User[] | Promise<User[]>;

    abstract allESLUsers(exceptUserId?: Nullable<number>): User[] | Promise<User[]>;

    abstract timeById(id: number): Nullable<SpecificTimeDTO> | Promise<Nullable<SpecificTimeDTO>>;

    abstract specificTimeEntryById(id: number): Nullable<TimeEntry> | Promise<Nullable<TimeEntry>>;

    abstract specificUserProfileDetail(id: number): Nullable<UserDTO> | Promise<Nullable<UserDTO>>;

    abstract timeEntriesByEmployeeId(id: number): Nullable<TimeEntryDTO[]> | Promise<Nullable<TimeEntryDTO[]>>;

    abstract timeEntries(date?: Nullable<string>, status?: Nullable<string>): TimeEntryDTO[] | Promise<TimeEntryDTO[]>;

    abstract timesheetSummary(startDate?: Nullable<string>, endDate?: Nullable<string>): TimeEntriesSummaryDTO[] | Promise<TimeEntriesSummaryDTO[]>;

    abstract allWorkInterruptionTypes(): WorkInterruptionType[] | Promise<WorkInterruptionType[]>;

    abstract interruptionsByTimeEntryId(interruption: ShowInterruptionRequestInput): WorkInterruptionDTO[] | Promise<WorkInterruptionDTO[]>;

    abstract allLeaves(): LeaveDTO[] | Promise<LeaveDTO[]>;

    abstract leaveTypes(): LeaveType[] | Promise<LeaveType[]>;

    abstract leaves(userId: number, year: number, leaveTypeId: number): LeavesDTO | Promise<LeavesDTO>;

    abstract leavesByDate(userId: number, date: string): LeavesDTO | Promise<LeavesDTO>;

    abstract yearlyAllLeaves(year: number, leaveTypeId: number): LeavesDTO | Promise<LeavesDTO>;

    abstract yearlyAllLeavesByDate(date: string): LeavesDTO | Promise<LeavesDTO>;

    abstract paidLeaves(id: number): number | Promise<number>;

    abstract userLeave(leaveId: number): LeaveDTO[] | Promise<LeaveDTO[]>;

    abstract projects(): Project[] | Promise<Project[]>;

    abstract allLeaders(projectId?: Nullable<number>): User[] | Promise<User[]>;

    abstract notificationByRecipientId(id: number): Notification[] | Promise<Notification[]>;

    abstract overtime(userId: number): MyOvertimeDTO[] | Promise<MyOvertimeDTO[]>;

    abstract allOvertime(): OvertimeDTO[] | Promise<OvertimeDTO[]>;

    abstract changeShiftByTimeEntry(timeEntryId: number): ChangeShiftRequest | Promise<ChangeShiftRequest>;

    abstract eslOffsetsByTimeEntry(timeEntryId: number, onlyUnused: boolean): ESLOffsetDTO[] | Promise<ESLOffsetDTO[]>;

    abstract allESLOffsets(isUsed?: Nullable<boolean>): ESLOffsetDTO[] | Promise<ESLOffsetDTO[]>;

    abstract eslChangeShiftByTimeEntry(timeEntryId: number): ESLChangeShiftRequest | Promise<ESLChangeShiftRequest>;

    abstract allEmployeeScheduleDetails(): EmployeeScheduleDTO[] | Promise<EmployeeScheduleDTO[]>;

    abstract employeeScheduleDetails(employeeScheduleId: number): EmployeeScheduleDTO[] | Promise<EmployeeScheduleDTO[]>;

    abstract employeesBySchedule(employeeScheduleId: number): UserDTO[] | Promise<UserDTO[]>;

    abstract employeeChangeScheduleRequest(userId: number): Nullable<ChangeScheduleRequest[]> | Promise<Nullable<ChangeScheduleRequest[]>>;

    abstract searchEmployeesBySchedule(request: SearchEmployeesByScheduleRequestInput): UserDTO[] | Promise<UserDTO[]>;
}

export class Role {
    id: number;
    name?: Nullable<string>;
    users: User[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class SpecificTimeDTO {
    media?: Nullable<File[]>;
    id: number;
    timeHour?: Nullable<TimeSpan>;
    remarks?: Nullable<string>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class SubscriptionObjectType {
    subscribeAllLeave: Leave;
    overtimeSummaryCreated?: Notification;
    leaveCreated?: LeaveNotification;
    overtimeCreated?: OvertimeNotification;
    changeShiftCreated?: ChangeShiftNotification;
    eslChangeShiftCreated?: ESLChangeShiftNotification;
    eslOffsetCreated?: ESLOffsetNotification;
    notificationCreated?: Notification;
}

export class Time {
    id: number;
    timeHour: TimeSpan;
    remarks?: Nullable<string>;
    media?: Nullable<Media[]>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class TimeDTO {
    timeHour?: Nullable<string>;
    id: number;
    remarks?: Nullable<string>;
    media?: Nullable<Media[]>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class TimeEntriesSummaryDTO {
    user: UserDTO;
    leave: number;
    absences: number;
    late?: Nullable<number>;
    undertime?: Nullable<number>;
    overtime?: Nullable<number>;
}

export class TimeEntry {
    id: number;
    userId: number;
    timeInId?: Nullable<number>;
    timeOutId?: Nullable<number>;
    startTime: TimeSpan;
    endTime: TimeSpan;
    breakStartTime: TimeSpan;
    breakEndTime: TimeSpan;
    date: DateTime;
    workedHours?: Nullable<string>;
    trackedHours: TimeSpan;
    user: User;
    timeIn?: Nullable<Time>;
    timeOut?: Nullable<Time>;
    overtime?: Nullable<Overtime>;
    changeShiftRequest?: Nullable<ChangeShiftRequest>;
    workInterruptions: WorkInterruption[];
    eslOffsets?: Nullable<ESLOffset[]>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class TimeEntryDTO {
    user: UserDTO;
    startTime?: Nullable<string>;
    endTime?: Nullable<string>;
    workedHours?: Nullable<string>;
    trackedHours?: Nullable<string>;
    timeIn?: Nullable<TimeDTO>;
    timeOut?: Nullable<TimeDTO>;
    date: Date;
    late: number;
    undertime: number;
    status: string;
    isLeaderApproved?: Nullable<boolean>;
    changeShift?: Nullable<ChangeShiftDTO>;
    eslChangeShift?: Nullable<ESLChangeShiftDTO>;
    id: number;
    userId: number;
    timeInId?: Nullable<number>;
    timeOutId?: Nullable<number>;
    breakStartTime: TimeSpan;
    breakEndTime: TimeSpan;
    overtime?: Nullable<Overtime>;
    changeShiftRequest?: Nullable<ChangeShiftRequest>;
    workInterruptions: WorkInterruption[];
    eslOffsets?: Nullable<ESLOffset[]>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class User {
    id: number;
    name?: Nullable<string>;
    email?: Nullable<string>;
    roleId: number;
    positionId: number;
    employeeScheduleId: number;
    paidLeaves: number;
    isOnline: boolean;
    profileImageId?: Nullable<number>;
    role: Role;
    position: Position;
    employeeSchedule: EmployeeSchedule;
    timeEntries: TimeEntry[];
    overtimes: Overtime[];
    profileImage?: Nullable<Media>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class UserDTO {
    timeEntry?: Nullable<TimeEntry>;
    avatarLink?: Nullable<string>;
    id: number;
    name?: Nullable<string>;
    email?: Nullable<string>;
    roleId: number;
    positionId: number;
    employeeScheduleId: number;
    paidLeaves: number;
    isOnline: boolean;
    profileImageId?: Nullable<number>;
    role: Role;
    position: Position;
    employeeSchedule: EmployeeSchedule;
    timeEntries: TimeEntry[];
    overtimes: Overtime[];
    profileImage?: Nullable<Media>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class WorkInterruption {
    id: number;
    timeEntryId: number;
    workInterruptionTypeId?: Nullable<number>;
    otherReason?: Nullable<string>;
    timeOut?: Nullable<TimeSpan>;
    timeIn?: Nullable<TimeSpan>;
    remarks?: Nullable<string>;
    workInterruptionType?: Nullable<WorkInterruptionType>;
    timeEntry?: Nullable<TimeEntry>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class WorkInterruptionDTO {
    timeOut?: Nullable<string>;
    timeIn?: Nullable<string>;
    id: number;
    timeEntryId: number;
    workInterruptionTypeId?: Nullable<number>;
    otherReason?: Nullable<string>;
    remarks?: Nullable<string>;
    workInterruptionType?: Nullable<WorkInterruptionType>;
    timeEntry?: Nullable<TimeEntry>;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class WorkInterruptionType {
    id: number;
    name?: Nullable<string>;
    workInterruption: WorkInterruption[];
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export class WorkingDayTime {
    id: number;
    employeeScheduleId: number;
    day?: Nullable<string>;
    from: TimeSpan;
    to: TimeSpan;
    breakFrom: TimeSpan;
    breakTo: TimeSpan;
    employeeSchedule: EmployeeSchedule;
    createdAt?: Nullable<DateTime>;
    updatedAt?: Nullable<DateTime>;
}

export type DateTime = any;
export type TimeSpan = any;
export type Upload = any;

export class ISchema {
    Query: IQuery;
    Mutation: IMutation;
    SubscriptionObjectType: SubscriptionObjectType;
}

type Nullable<T> = T | null;