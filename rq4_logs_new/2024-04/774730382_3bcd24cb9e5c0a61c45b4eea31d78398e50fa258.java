package Happy20.GrowingPetPlant.Arduino.Controller;

import Happy20.GrowingPetPlant.Arduino.Service.ArduinoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("arduino")
public class ArduinoController {
    private final ArduinoService arduinoService;

    @Autowired
    public ArduinoController(ArduinoService arduinoService) {
        this.arduinoService = arduinoService;
    }

    // 물 주기 api
    @PostMapping("/watering")
    public String Watering() {
        if (arduinoService.wateringPlant())
            return "Watering Plant";
        else
            return "Error";
    }

    // 조명 on api
    @PostMapping("/lighton")
    public String LightOn() {
        return "Light On";
    }

    // 조명 off api
    @PostMapping("/lightoff")
    public String LightOff() {
        return "Light Off";
    }

    // 팬 on api
    @PostMapping("/fanon")
    public String FanOn() {
        return "Fan On";
    }

    // 팬 off api
    @PostMapping("/fanoff")
    public String FanOff() {
        return "Fan Off";
    }
}