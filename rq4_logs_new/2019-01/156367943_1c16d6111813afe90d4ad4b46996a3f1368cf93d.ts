import {Patient} from "../models/Patient";

export class PatientController {

    public static abbassaReputazione(paziente: Patient, data: Date): void {
        paziente.reputazione = PatientController.calcolaReputazione(paziente.reputazione, data);
    }
    private static calcolaReputazione(reputazione: number , data: Date): number {
        // calcolo distanza da adesso alla data della visita
        const diff: number = +(data.getTime() - new Date().getTime()).toFixed(3);
        const newRep: number = reputazione - (+(100 / diff).toFixed(3));
        return newRep;
    }

}