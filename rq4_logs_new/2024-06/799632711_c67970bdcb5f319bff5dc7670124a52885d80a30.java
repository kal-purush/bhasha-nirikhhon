package com.LuuQu.medicalclinic.factory;

import com.LuuQu.medicalclinic.model.dto.DoctorDto;
import com.LuuQu.medicalclinic.model.dto.FacilityDto;
import com.LuuQu.medicalclinic.model.dto.PatientDto;
import com.LuuQu.medicalclinic.model.entity.Doctor;
import com.LuuQu.medicalclinic.model.entity.Facility;
import com.LuuQu.medicalclinic.model.entity.Patient;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class TestData {
    public static class FacilityFactory {
        public static List<Facility> getList(int amount) {
            List<Facility> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<Facility> getList() {
            return getList(2);
        }

        public static Facility get(Long id) {
            Facility facility = get();
            facility.setId(id);
            return facility;
        }

        public static Facility get() {
            Facility facility = new Facility();
            facility.setName("Name");
            facility.setCity("City");
            facility.setZipCode("zipCode");
            facility.setStreet("street");
            facility.setBuildingNo("123");
            return facility;
        }
    }

    public static class FacilityDtoFactory {
        public static List<FacilityDto> getList(int amount) {
            List<FacilityDto> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<FacilityDto> getList() {
            return getList(2);
        }

        public static FacilityDto get(Long id) {
            FacilityDto facilityDto = get();
            facilityDto.setId(id);
            return facilityDto;
        }

        public static FacilityDto get() {
            FacilityDto facilityDto = new FacilityDto();
            facilityDto.setName("Name");
            facilityDto.setCity("City");
            facilityDto.setZipCode("zipCode");
            facilityDto.setStreet("street");
            facilityDto.setBuildingNo("123");
            return facilityDto;
        }
    }

    public static class PatientFactory {
        public static List<Patient> getList(int amount) {
            List<Patient> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<Patient> getList() {
            return getList(2);
        }

        public static Patient get(Long id) {
            Patient patient = get();
            patient.setId(id);
            return patient;
        }

        public static Patient get() {
            Patient patient = new Patient();
            patient.getUser().setEmail("email");
            patient.getUser().setPassword("password");
            patient.setIdCardNo("idCardNo");
            patient.getUser().setFirstName("firstName");
            patient.getUser().setLastName("lastName");
            patient.setPhoneNumber("123123123");
            patient.setBirthday(LocalDate.of(2000, 1, 1));
            return patient;
        }
    }

    public static class PatientDtoFactory {
        public static List<PatientDto> getList(int amount) {
            List<PatientDto> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<PatientDto> getList() {
            return getList(2);
        }

        public static PatientDto get(Long id) {
            PatientDto patientDto = get();
            patientDto.setId(id);
            return patientDto;
        }

        public static PatientDto get() {
            PatientDto patientDto = new PatientDto();
            patientDto.setEmail("email");
            patientDto.setPassword("password");
            patientDto.setIdCardNo("idCardNo");
            patientDto.setFirstName("firstName");
            patientDto.setLastName("lastName");
            patientDto.setPhoneNumber("123123123");
            patientDto.setBirthday(LocalDate.of(2000, 1, 1));
            return patientDto;
        }
    }

    public static class DoctorFactory {
        public static List<Doctor> getList(int amount) {
            List<Doctor> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<Doctor> getList() {
            return getList(2);
        }

        public static Doctor get(Long id) {
            Doctor doctor = get();
            doctor.setId(id);
            return doctor;
        }

        public static Doctor get() {
            Doctor doctor = new Doctor();
            doctor.getUser().setEmail("email");
            doctor.getUser().setPassword("password");
            doctor.getUser().setFirstName("firstName");
            doctor.getUser().setLastName("lastName");
            doctor.setSpecialization("specialization");
            return doctor;
        }
    }

    public static class DoctorDtoFactory {
        public static List<DoctorDto> getList(int amount) {
            List<DoctorDto> list = new ArrayList<>();
            for (int i = 0; i < amount; i++) {
                list.add(get((long) i));
            }
            return list;
        }

        public static List<DoctorDto> getList() {
            return getList(2);
        }

        public static DoctorDto get(Long id) {
            DoctorDto doctorDto = get();
            doctorDto.setId(id);
            return doctorDto;
        }

        public static DoctorDto get() {
            DoctorDto doctorDto = new DoctorDto();
            doctorDto.setEmail("email");
            doctorDto.setPassword("password");
            doctorDto.setFirstName("firstName");
            doctorDto.setLastName("lastName");
            doctorDto.setSpecialization("specialization");
            return doctorDto;
        }
    }
}