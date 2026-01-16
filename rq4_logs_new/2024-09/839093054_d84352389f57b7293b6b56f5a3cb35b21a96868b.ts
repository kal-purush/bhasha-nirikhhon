import { api } from "~/trpc/react"

export function getRecords() {
    return (
        api.patientRecord.getPatientRecords.useQuery()
    );
}