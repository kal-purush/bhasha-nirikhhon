package Controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import Util.VideoCut;

@Controller
@RequestMapping("/video")
public class VideoWeb {

  @RequestMapping(value="/cut")
  @ResponseBody
  public int cut(@RequestParam String url){
    VideoCut cut = new VideoCut();
    return cut.cut(url,"10");
  }

  @RequestMapping(value="/Icut")
  @ResponseBody
  public int icut(@RequestParam String url){
    VideoCut cut = new VideoCut();
    return cut.InstanceCut(url);
  }
}