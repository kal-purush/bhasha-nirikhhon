package com.Medverse.Login.Entity;

import com.Medverse.Login.Entity.PatientEntity.Gender;
import com.Medverse.Login.Entity.PatientEntity.Role;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

@Entity
@Table(name = "doctors")
public class DoctorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long doctorId;

    @NotBlank(message = "First name is mandatory")
    private String firstName;

    @NotBlank(message = "Last name is mandatory")
    private String lastName;

    @Email(message = "Email should be valid")
    @NotBlank(message = "Email is mandatory")
    private String email;

    @NotBlank(message = "Password is required")
    @Column(nullable = false, length = 255)
    private String password;

    @NotBlank(message = "Specialization is mandatory")
    private String specialization;

    @Min(value = 0, message = "Experience should be positive")
    private int experienceYears;

    @NotBlank(message = "Phone number is mandatory")
    private String phoneNumber;

    @NotBlank(message = "Availability schedule is mandatory")
    private String availabilitySchedule;

    private long hospitalId;

    @NotBlank(message = "MCR number is mandatory")
    private String mcrNumber;

    @Enumerated(EnumType.STRING)
    private Role role;

    @Enumerated(EnumType.STRING)
    private Gender gender;

    

    public DoctorEntity() {
		super();
		// TODO Auto-generated constructor stub
	}

	// Constructor with all fields (excluding password, as it will be set later)
    public DoctorEntity(long doctorId, @NotBlank(message = "First name is mandatory") String firstName,
                        @NotBlank(message = "Last name is mandatory") String lastName,
                        @Email(message = "Email should be valid") @NotBlank(message = "Email is mandatory") String email,
                        @NotBlank(message = "Password is required") String password,
                        @NotBlank(message = "Specialization is mandatory") String specialization,
                        @Min(value = 0, message = "Experience should be positive") int experienceYears,
                        @NotBlank(message = "Phone number is mandatory") String phoneNumber,
                        @NotBlank(message = "Availability schedule is mandatory") String availabilitySchedule, long hospitalId,
                        @NotBlank(message = "MCR number is mandatory") String mcrNumber, Role role, Gender gender) {
        this.doctorId = doctorId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.password = password;
        this.specialization = specialization;
        this.experienceYears = experienceYears;
        this.phoneNumber = phoneNumber;
        this.availabilitySchedule = availabilitySchedule;
        this.hospitalId = hospitalId;
        this.mcrNumber = mcrNumber;
        this.role = role;
        this.gender = gender;
    }

    public DoctorEntity(Long doctorId) {
        this.doctorId = doctorId;
    }
    // Getters and Setters
    public long getDoctorId() {
        return doctorId;
    }

    public void setDoctorId(long doctorId) {
        this.doctorId = doctorId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getSpecialization() {
        return specialization;
    }

    public void setSpecialization(String specialization) {
        this.specialization = specialization;
    }

    public int getExperienceYears() {
        return experienceYears;
    }

    public void setExperienceYears(int experienceYears) {
        this.experienceYears = experienceYears;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getAvailabilitySchedule() {
        return availabilitySchedule;
    }

    public void setAvailabilitySchedule(String availabilitySchedule) {
        this.availabilitySchedule = availabilitySchedule;
    }

    public long getHospitalId() {
        return hospitalId;
    }

    public void setHospitalId(long hospitalId) {
        this.hospitalId = hospitalId;
    }

    public String getMcrNumber() {
        return mcrNumber;
    }

    public void setMcrNumber(String mcrNumber) {
        this.mcrNumber = mcrNumber;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}