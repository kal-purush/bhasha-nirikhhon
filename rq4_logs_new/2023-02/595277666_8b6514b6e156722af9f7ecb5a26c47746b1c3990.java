package gigjob.controller;

import gigjob.model.request.JobRequest;
import gigjob.model.response.JobResponse;
import gigjob.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.modelmapper.ModelMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Log4j2
@RestController
@RequestMapping(value = "/api")
@RequiredArgsConstructor
public class JobController {
    private final ModelMapper modelMapper;
    private final JobService jobService;

    @GetMapping("/v1/job")
    public ResponseEntity<List<JobResponse>> getJobList() {
        List<JobResponse> jobResponseList = jobService.getJob();
        return ResponseEntity.status(HttpStatus.OK).body(jobResponseList);
    }

    @GetMapping("/v1/job/redis")
    public ResponseEntity<List<JobResponse>> getJobListRedis() {
        List<JobResponse> jobResponseList = jobService.getJobListRedis();
        return ResponseEntity.status(HttpStatus.OK).body(jobResponseList);
    }

    @GetMapping("/v1/job/{id}")
    public ResponseEntity<JobResponse> getJobById(@PathVariable Long id) {
        return ResponseEntity.status(HttpStatus.OK).body(jobService.getJobById(id));
    }

    @PostMapping("/v1/job")
    public ResponseEntity<JobResponse> createJob(@RequestBody JobRequest jobRequest) {
        return ResponseEntity.status(HttpStatus.OK).body(jobService.addJob(jobRequest));
    }

    @PatchMapping("/v1/job")
    public ResponseEntity<JobResponse> updateJob(@RequestBody JobRequest jobRequest) {
        return ResponseEntity.status(HttpStatus.OK).body(jobService.updateJob(jobRequest));
    }
}