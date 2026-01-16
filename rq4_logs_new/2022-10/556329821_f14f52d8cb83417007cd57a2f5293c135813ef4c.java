package oit.is.z1452.kaizi.janken.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class JankenController {

  @PostMapping("/janken")
  public String janken(@RequestParam String name, ModelMap model) {

    name = "Hi " + name;
    model.addAttribute("name", name);
    return "janken.html";
  }

  @GetMapping("/janken/{param1}")
  public String sample22(@PathVariable String param1, ModelMap model) {

    String myHand = "あなたの手 " + param1;
    String opponentHand = "相手の手 Gu";
    String result = "";
    if (param1.equals("Gu")) {
      result = "Draw";
    } else if (param1.equals("Choki")) {
      result = "You Lose";
    } else if (param1.equals("Pa")) {
      result = "You Win!";
    }
    model.addAttribute("myHand", myHand);
    model.addAttribute("opponentHand", opponentHand);
    model.addAttribute("result", result);
    return "janken.html";

  }

}