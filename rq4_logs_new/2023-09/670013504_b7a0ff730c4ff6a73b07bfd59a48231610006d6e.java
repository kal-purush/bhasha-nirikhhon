package com.grievance.controller;

import com.grievance.dto.TicketInDto;
import com.grievance.dto.TicketOutDto;
import com.grievance.service.TicketService;

import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 */
@CrossOrigin("*")
@RestController
@RequestMapping("/ticket")
public class TicketController {
  /**
   *
   */
  @Autowired
  private TicketService ticketService;

  /**
   *
   * Controller method to create a new ticket.
   * @param ticketInDto
   * @return Responseentity with optional of TicketOut DTO.
   */
  @PostMapping("/addTicket")
  public ResponseEntity<?> saveTicket(
     @RequestBody final TicketInDto ticketInDto) {
     Optional<TicketOutDto> optionalTicketOutDto =
         ticketService.saveTicket(ticketInDto);
    return new ResponseEntity<>(optionalTicketOutDto, HttpStatus.ACCEPTED);
  }

  /**
   * controller method to return list of all tickets.
   *
   * @return Responseentity with optional of list of all tickets.
   */
  @GetMapping("/listAllTickets")
  public ResponseEntity<?> listAllTickets() {
      Optional<List<TicketOutDto>>
      optional = ticketService.listOfAllTickets();
      return new ResponseEntity<>(optional, HttpStatus.ACCEPTED);
  }
}