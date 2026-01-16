package dbpractice.dbhomework.controller;

import dbpractice.dbhomework.domain.Address;
import dbpractice.dbhomework.domain.Booking;
import dbpractice.dbhomework.domain.Customer;
import dbpractice.dbhomework.repository.BookingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

@RequestMapping("/bookings")
@RestController
@RequiredArgsConstructor
public class BookingController {
    public final BookingRepository bookingRepository;

    @GetMapping(value = "/bookings/new")
    public String createForm(Model model) {
        model.addAttribute("bookingForm", new BookingForm());
        return "bookings/createCustomerForm";
    }

    @PostMapping("/bookings/new")
    public String create(@Valid CustomerForm form, BindingResult result) {
        if (result.hasErrors()) {
            return "bookings/createCustomerForm";
        }

        return "redirect:/";
    }
    //    홈화면에서 booking의 리스트를 조회하려고 할 때..?!
    @GetMapping(value = "/bookings")
    public String list(Model model) {
        List<Booking> bookings = bookingRepository.findAll();
        model.addAttribute("bookings", bookings);
        return "/bookings/bookingList";
    }
}