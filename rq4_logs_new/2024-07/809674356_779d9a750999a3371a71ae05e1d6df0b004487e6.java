package controller;

import javax.inject.Inject;
import javax.servlet.http.HttpSession;

import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import service.IF_ReportService;
import vo.ReportVO;

@Controller
@EnableAsync
public class ReportController {
	@Inject
	IF_ReportService rser;
	
	@PostMapping("reportPost")
	@ResponseBody
	public void reportPost(@ModelAttribute ReportVO rvo,Model model,HttpSession session) throws Exception {
		rvo.setId(String.valueOf(session.getAttribute("userid")));
		rser.Report(rvo);
	}
	
}