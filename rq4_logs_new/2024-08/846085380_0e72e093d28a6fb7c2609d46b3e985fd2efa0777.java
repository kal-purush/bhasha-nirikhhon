package vn.hust.hedspi.ezsport.controllers;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import vn.hust.hedspi.ezsport.dtos.fieldOrder.CreateFieldOrderRequest;
import vn.hust.hedspi.ezsport.dtos.fieldOrder.UpdateFieldOrderRequest;
import vn.hust.hedspi.ezsport.entities.Field;
import vn.hust.hedspi.ezsport.entities.FieldOrder;
import vn.hust.hedspi.ezsport.entities.User;
import vn.hust.hedspi.ezsport.repositories.FieldOrderRepository;
import vn.hust.hedspi.ezsport.repositories.FieldRepository;
import vn.hust.hedspi.ezsport.repositories.UserRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@RestController
@NoArgsConstructor
@AllArgsConstructor
@RequestMapping("api/v1/field-order")
public class FieldOrderController {
    @Autowired
    private FieldOrderRepository fieldOrderRepository;

    @Autowired
    private FieldRepository fieldRepository;

    @Autowired
    private UserRepository userRepository;

    //List field order
    @GetMapping()
    public ResponseEntity<List<FieldOrder>> getAllFieldOrders(){
        return ResponseEntity.ok(fieldOrderRepository.findAll());
    }

    //Create
    @PostMapping()
    public ResponseEntity<FieldOrder> createFieldOrder(@Valid @RequestBody CreateFieldOrderRequest requestBody) {
        Field field = fieldRepository.findById(requestBody.getFieldId()).orElse(null);
        if (field == null){
            return ResponseEntity.notFound().build();
        }

        if (requestBody.getStart().isAfter(requestBody.getEnd())){
            return ResponseEntity.badRequest().build();
        }

        if (requestBody.getUserId() != null){
            User user = userRepository.findById(requestBody.getUserId()).orElse(null);
            if (user == null){
                return ResponseEntity.notFound().build();
            }
        }

        FieldOrder fieldOrder = new FieldOrder();
        fieldOrder.setField(field);
        fieldOrder.setStart(requestBody.getStart());
        fieldOrder.setEnd(requestBody.getEnd());
        fieldOrder.setDate(requestBody.getDate());
        fieldOrder.setPrice(requestBody.getPrice());
        fieldOrder.setUserId(requestBody.getUserId());
        fieldOrder.setPaidAt(requestBody.getPaidAt());

        return ResponseEntity.ok(fieldOrderRepository.save(fieldOrder));
    }

    //Show
    @GetMapping("/{id}")
    public ResponseEntity<FieldOrder> getFieldOrderById(@PathVariable UUID id){
        Optional<FieldOrder> fieldOrder = fieldOrderRepository.findById(id);

        return fieldOrder.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    //Update
    @PutMapping("/{id}")
    public ResponseEntity<FieldOrder> updateFieldOrder(@PathVariable UUID id, @Valid @RequestBody UpdateFieldOrderRequest requestBody) {
        Optional<FieldOrder> fieldOrderOptional = fieldOrderRepository.findById(id);
        if (fieldOrderOptional.isEmpty()){
            return ResponseEntity.notFound().build();
        }
        if (requestBody.getStart().isAfter(requestBody.getEnd())){
            return ResponseEntity.badRequest().build();
        }
        if (requestBody.getUserId() != null){
            User user = userRepository.findById(requestBody.getUserId()).orElse(null);
            if (user == null){
                return ResponseEntity.notFound().build();
            }
        }

        FieldOrder fieldOrder = fieldOrderOptional.get();
        fieldOrder.setField(fieldRepository.findById(requestBody.getFieldId()).orElse(null));
        fieldOrder.setStart(requestBody.getStart());
        fieldOrder.setEnd(requestBody.getEnd());
        fieldOrder.setDate(requestBody.getDate());
        fieldOrder.setPrice(requestBody.getPrice());
        fieldOrder.setUserId(requestBody.getUserId());
        fieldOrder.setPaidAt(requestBody.getPaidAt());

        return ResponseEntity.ok(fieldOrderRepository.save(fieldOrder));
    }

    //Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteFieldOrder(@PathVariable UUID id){
        Optional<FieldOrder> fieldOrderOptional = fieldOrderRepository.findById(id);
        if (fieldOrderOptional.isEmpty()){
            return ResponseEntity.notFound().build();
        }
        fieldOrderRepository.delete(fieldOrderOptional.get());

        return ResponseEntity.noContent().build();
    }
}