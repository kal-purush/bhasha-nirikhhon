package mz.org.fgh.sifmoz.backend.tansreference;

import mz.org.fgh.sifmoz.backend.clinic.Clinic;
import mz.org.fgh.sifmoz.backend.patientIdentifier.PatientServiceIdentifier;
import mz.org.fgh.sifmoz.backend.patientVisit.PatientVisit;
import mz.org.fgh.sifmoz.backend.patientVisitDetails.PatientVisitDetails;

public class TransReferenceData {


    Clinic originClinic;
    Clinic destinationClinic;
    PatientServiceIdentifier patientServiceIdentifier;
    PatientVisit patientVisit;
    PatientVisitDetails patientVisitDetails;

    public Clinic getOriginClinic() {
        return originClinic;
    }

    public void setOriginClinic(Clinic originClinic) {
        this.originClinic = originClinic;
    }

    public Clinic getDestinationClinic() {
        return destinationClinic;
    }

    public void setDestinationClinic(Clinic destinationClinic) {
        this.destinationClinic = destinationClinic;
    }

    public PatientServiceIdentifier getPatientServiceIdentifier() {
        return patientServiceIdentifier;
    }

    public void setPatientServiceIdentifier(PatientServiceIdentifier patientServiceIdentifier) {
        this.patientServiceIdentifier = patientServiceIdentifier;
    }

    public PatientVisit getPatientVisit() {
        return patientVisit;
    }

    public void setPatientVisit(PatientVisit patientVisit) {
        this.patientVisit = patientVisit;
    }
}