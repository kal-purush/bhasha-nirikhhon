package ru.tadzh.iss.controller;

import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import ru.tadzh.iss.NotFoundException;
import ru.tadzh.iss.dto.history.HistoryDto;
import ru.tadzh.iss.dto.securities.SecuritiesDto;
import ru.tadzh.iss.dto.HistoryAndSecuritiesListParams;
import ru.tadzh.iss.service.HistoryAndSecuritiesService;

import java.util.List;

@Controller
@RequestMapping("/historyAndSecurities")
public class HistoryAndSecuritiesController {

    @Autowired
    HistoryAndSecuritiesService historyAndSecuritiesService;

//        Отображение страницы содержащей Историю + Бумаги

    @GetMapping
    public String listPageHistoryAndSecurities(Model model, HistoryAndSecuritiesListParams historyAndSecuritiesListParams) {
        model.addAttribute("historyAndSecurities", historyAndSecuritiesService.findAllHistoryAndSecuritiesWithParam(historyAndSecuritiesListParams));
        return "historyAndSecurities";
    }

    //    Создание новой Истории

    @GetMapping("/newHistory")
    public String newHistory(Model model) {
        model.addAttribute("history", new HistoryDto());
        model.addAttribute("securities", historyAndSecuritiesService.findAllSecurities());
        return "history_form";
    }

    //    Редактирование Истории

    @GetMapping("/historyUpdate/{idHistory}")
    public String editHistory(@PathVariable("idHistory") Long id, Model model) {
        model.addAttribute("history", historyAndSecuritiesService.findByIdHistory(id)
                .orElseThrow(() -> new NotFoundException("History not found")));
        model.addAttribute("securities", historyAndSecuritiesService.findAllSecurities());
        return "history_form";
    }

//        Сохранение новой или сохранение изменений в Истории

    @PostMapping("/historyUpdate")
    public String updateHistory(@Valid @ModelAttribute("history") HistoryDto history, BindingResult result, Model model) {
        List<SecuritiesDto> allList = historyAndSecuritiesService.findAllSecurities();
        if (history.getSecuritiesDto()==null){
            for (SecuritiesDto sec : allList) {
                if (sec.getSecId().equals(history.getSecId())){
                    history.setSecuritiesDto(sec);
                    break;
                }
                else
                    model.addAttribute("securities", allList);
                return "history_form";
            }
        }
        if (result.hasErrors()) {
            model.addAttribute("securities", allList);
            return "history_form";
        }
        historyAndSecuritiesService.saveHistory(history);
        return "redirect:/historyAndSecurities";
    }

    //  Удаление Истории

    @DeleteMapping("/historyDelete/{idHistory}")
    public String deleteHistory(@PathVariable("idHistory") Long id) {
        historyAndSecuritiesService.deleteByIdHistory(id);
        return "redirect:/historyAndSecurities";
    }

    //    Создание новой Бумаги

    @GetMapping("/newSecurities")
    public String newSecurities(Model model) {
        model.addAttribute("securities", new SecuritiesDto());
        return "securities_form";
    }

    //    Редактирование Бумаги
    @GetMapping("/securitiesUpdate/{idSecurities}")
    public String editSecurities(@PathVariable("idSecurities") String id, Model model) {
        model.addAttribute("securities", historyAndSecuritiesService.findByIdSecurities(id)
                .orElseThrow(() -> new NotFoundException("Securities not found")));
        return "securities_form";
    }

//        Сохранение новой или сохранение изменений в Бумаги

    @PostMapping("/securitiesUpdate")
    public String updateSecurities(@Valid @ModelAttribute("securities") SecuritiesDto securities, BindingResult result) {
//        model.addAttribute("securities", new SecuritiesDto());
        if (result.hasErrors()) {
            return "securities_form";
        }
        System.out.println(securities);
        historyAndSecuritiesService.saveSecurities(securities);
        return "redirect:/historyAndSecurities";
    }

    //    Удаление Бумаги

    @DeleteMapping("/securitiesDelete/{idSecurities}")
    public String deleteSecurities(@PathVariable("idSecurities") String id) {
        historyAndSecuritiesService.deleteByIdSecurities(id);
        return "redirect:/historyAndSecurities";
    }
}