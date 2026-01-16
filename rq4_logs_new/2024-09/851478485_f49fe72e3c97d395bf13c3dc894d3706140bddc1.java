package org.detective.controller.Receive;

import org.detective.dto.AssignedRequestDTO;
import org.detective.dto.ReceiveRequestDTO;
import org.detective.services.Receive.ReceiveService;
import org.springframework.http.converter.json.GsonBuilderUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/receive")
public class ReceiveController {

    private ReceiveService receiveService;

    public ReceiveController(ReceiveService receiveService) {
        this.receiveService = receiveService;
    }



    @GetMapping("")
    public List<ReceiveRequestDTO> getAssingedRequest(@RequestParam(value = "email",required = true) String email) {
        System.err.println("Receive Controller");
        List<ReceiveRequestDTO> assignedRequest = receiveService.getAssignedRequest(email);
        System.out.println("Receive Controller : " + assignedRequest.toString());
        System.out.println("Receive Controller : " + assignedRequest.size());
        return assignedRequest;
    }

    @GetMapping("/detail/{requestId}")
    public AssignedRequestDTO getRequestDetail(@PathVariable("requestId") Long requestId, @RequestParam(value = "email",required = true) String email) {
        System.err.println("Receive Controller2");
        return receiveService.getRequestDetail(requestId, email);
    }
}