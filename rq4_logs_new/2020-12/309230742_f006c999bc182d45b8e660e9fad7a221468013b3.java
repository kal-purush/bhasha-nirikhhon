package co.kr.hsh;

import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping("/test/*")
public class TestC {

    //게시글 리스트 조회
    @RequestMapping(value = "list")
    public String boardList(@RequestParam Map<String, Object> paramMap, Model model) {

        return "boardList";
    }

}