package studentenrollment.familyinfo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import studentenrollment.StudentinfoApi.Repository.StudentRepository;
import studentenrollment.familyinfo.model.Parents;
import studentenrollment.familyinfo.repository.ParentsRepository;



@Service
public class ParentsServiceImpl implements ParentsService {
    private final ParentsRepository parentsRepository;
    private final StudentRepository studentRepository;

    @Autowired
    public ParentsServiceImpl(ParentsRepository parentsRepository, StudentRepository studentRepository) {
        this.parentsRepository = parentsRepository;
        this.studentRepository = studentRepository;
    }

    @Override
    public Parents storeParentDetails(int studentId, String fatherName, String motherName, Long contactNumber) {
        // Check if the studentId exists in the student table
        if (!studentRepository.existsById(studentId)) {
            throw new IllegalArgumentException("Invalid student ID: " + studentId);
        }

        Parents existingParents = parentsRepository.findByStudentId(studentId);
        // Create new Parents instance if no record exists
        if (existingParents == null) {
            existingParents = new Parents();
            existingParents.setStudentId(studentId);
        }

        existingParents.setFatherName(fatherName);
        existingParents.setMotherName(motherName);
        existingParents.setContactNumber(contactNumber);

        return parentsRepository.save(existingParents);
    }
}


