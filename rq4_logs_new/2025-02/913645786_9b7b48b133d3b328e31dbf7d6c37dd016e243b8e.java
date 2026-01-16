package com.Medverse.Login.Entity;

import jakarta.persistence.*;

@Entity
public class DoctorDocument {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "userId", referencedColumnName = "userId", nullable = false) // Correct reference to PatientEntity
    private PatientEntity patient;

    @ManyToOne
    @JoinColumn(name = "doctorId", referencedColumnName = "doctorId", nullable = false) // Correct reference to DoctorEntity
    private DoctorEntity doctor;

    @Lob
    @Column(name = "document", columnDefinition = "LONGBLOB")
    private byte[] document;

    private String fileName;

    public DoctorDocument() {
    }

    public DoctorDocument(PatientEntity patient, DoctorEntity doctor, byte[] document, String fileName) {
        this.patient = patient;
        this.doctor = doctor;
        this.document = document;
        this.fileName = fileName;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public PatientEntity getPatient() {
        return patient;
    }

    public void setPatient(PatientEntity patient) {
        this.patient = patient;
    }

    public DoctorEntity getDoctor() {
        return doctor;
    }

    public void setDoctor(DoctorEntity doctor) {  // Fixed incorrect assignment
        this.doctor = doctor;
    }

    public byte[] getDocument() {
        return document;
    }

    public void setDocument(byte[] document) {
        this.document = document;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}