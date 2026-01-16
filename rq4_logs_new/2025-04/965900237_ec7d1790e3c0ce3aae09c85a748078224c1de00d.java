package skillfactory.DreamTeam.globus.it.controllers;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import skillfactory.DreamTeam.globus.it.dto.operation.CancelOperationRequest;
import skillfactory.DreamTeam.globus.it.dto.operation.ConfirmOperationRequest;
import skillfactory.DreamTeam.globus.it.dto.operation.CreateOperationRequest;
import skillfactory.DreamTeam.globus.it.dto.operation.DeleteOperationRequest;
import skillfactory.DreamTeam.globus.it.services.operation.OperationService;

@RestController
@RequiredArgsConstructor
public class OperationController {
    private final OperationService operationService;


     @PostMapping("/operations/create")
     public ResponseEntity createOperation(@RequestBody CreateOperationRequest request) {
         return ResponseEntity.ok(operationService.createOperation(request));
     }

    @PostMapping("/operations/cancel")
    public ResponseEntity cancelOperation(@RequestBody CancelOperationRequest request) {
        return ResponseEntity.ok(operationService.cancelOperation(request));
    }

    @PostMapping("/operations/сonfirm")
    public ResponseEntity сonfirmOperation(@RequestBody ConfirmOperationRequest request) {
        return ResponseEntity.ok(operationService.confirmOperation(request));
    }

    @PostMapping("/operations/delete")
    public ResponseEntity deleteOperation(@RequestBody DeleteOperationRequest request) {
        return ResponseEntity.ok(operationService.deleteOperation(request));
    }

}