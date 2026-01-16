import { Patient } from "../domain/PatientType";
import * as patientMapper from "./mappers/patientMapper";

export const getPatient = async (fnr: string): Promise<Patient> => {
    console.log('hei')
    const result = await fetch(`http://192.168.1.6:3001/api/folk/hentPersonMedFNr/:${fnr}`,{
        method: 'GET',
        headers: {
          Accept: 'application/json'
        }
    });

    if (!result.ok) {
        throw new Error(`Call to folkeregisteret failed with status ${result.status} ${result.statusText}`);
    }

    const data = await result.json();
    const patient = patientMapper.mapDataToPatient(data);
    return patient;

};

