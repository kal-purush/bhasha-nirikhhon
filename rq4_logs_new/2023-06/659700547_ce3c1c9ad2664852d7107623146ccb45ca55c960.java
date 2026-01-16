package com.example.backendeindopdracht.controller;


import com.example.backendeindopdracht.DTO.inputDto.InvoiceInputDTO;
import com.example.backendeindopdracht.DTO.outputDto.InvoiceOutputDTO;
import com.example.backendeindopdracht.service.InvoiceService;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@AllArgsConstructor
@RestController
@RequestMapping("/invoices")
public class InvoiceController {

    private final InvoiceService invoiceService;

    //POST
    @PostMapping("/add")
    public ResponseEntity<InvoiceOutputDTO> addInvoice (@RequestBody InvoiceInputDTO invoiceInputDTO){
        InvoiceOutputDTO addedInvoice = invoiceService.addInvoice(invoiceInputDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(addedInvoice);
    }
}