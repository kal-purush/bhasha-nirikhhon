package com.example.audia6;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController

public class TuningController
{
    public int ps = 190;
    public String farbe = "Gruen";

    @GetMapping("/morepowerbaby")
    public String morePowerBaby(int a)
    {
        ps = a + ps;
        return "Dein Audi A5 hat " + ps + " ps";
    }

    @GetMapping("/color")
    public String changeColor(String c)
    {
        farbe = c;
        return farbe;
    }
}