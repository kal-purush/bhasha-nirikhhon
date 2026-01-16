export interface BloodRequestStatus {

    id: string,
    statusName: string,
    statusDescription: string,
    statusOrder: number,
    prevPossibleStatus: string,
    nextPossibleStatus: string,
    prevPossibleStatusId: string,
    nextPossibleStatusId: string    
}