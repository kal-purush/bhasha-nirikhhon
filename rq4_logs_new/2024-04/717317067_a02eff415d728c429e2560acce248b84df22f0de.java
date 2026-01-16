package sit.cp23ms2.sportconnect.controllers;

import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import sit.cp23ms2.sportconnect.dtos.TestFileStore;
import sit.cp23ms2.sportconnect.dtos.file.CreateFileDto;
import sit.cp23ms2.sportconnect.dtos.file.FileDto;
import sit.cp23ms2.sportconnect.dtos.file.PageFileDto;
import sit.cp23ms2.sportconnect.dtos.notification.NotificationDto;
import sit.cp23ms2.sportconnect.dtos.notification.PageNotificationDto;
import sit.cp23ms2.sportconnect.entities.File;
import sit.cp23ms2.sportconnect.exceptions.type.BadRequestException;
import sit.cp23ms2.sportconnect.exceptions.type.ForbiddenException;
import sit.cp23ms2.sportconnect.services.FileService;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;

@RestController
@RequestMapping("/api/files")
public class FileController {
    @Autowired
    FileService service;
    @Autowired
    ModelMapper modelMapper;

    @GetMapping
    public PageFileDto getNotification(@RequestParam(defaultValue = "0") int page,
                                       @RequestParam(defaultValue = "10") int pageSize,
                                       @RequestParam(required = false) Integer activityId) {

        return service.getFile(page, pageSize, activityId);

    }

    @GetMapping("/{id}")
    public FileDto getById(@PathVariable Integer id) {
        return modelMapper.map(service.getById(id), FileDto.class) ;
    }

    @PostMapping
    public FileDto upload(@Valid @ModelAttribute CreateFileDto newFile, HttpServletResponse response) throws IOException, BadRequestException {
        return modelMapper.map(service.create(newFile, response), FileDto.class);
    }

    @DeleteMapping("/{id}")
    public void delete(@PathVariable Integer id) throws IOException {
        service.delete(id);
    }

//    @PostMapping
//    public String upload(@Valid @ModelAttribute TestFileStore testFileStore) {
//        //return service.upload(testFileStore.getMultipartFile());
//        return "yo";
//    }
}