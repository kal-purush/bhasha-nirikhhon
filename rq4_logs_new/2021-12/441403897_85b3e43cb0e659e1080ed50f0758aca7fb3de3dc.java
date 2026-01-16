package com.spartenportal.controller;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.spartenportal.bean.AttendanceSheetBean;
import com.spartenportal.entity.AttendanceSheet;
import com.spartenportal.entity.User;
import com.spartenportal.service.AttendanceSheetService;

@RestController
public class AttendanceController {

	@Autowired
	AttendanceSheetService attendanceSheetService;

	@RequestMapping(value = "/AttendanceSheet")
	public ModelAndView viewAttendanceSheet(ModelAndView mv, Model m, HttpServletRequest request) {
		int userIdfk = (int) request.getSession().getAttribute("userId");
		List<AttendanceSheet> sheets = attendanceSheetService.getSheetByuserIdFk(userIdfk);
		if (sheets == null) {
			return mv;
		} else {
			m.addAttribute("sheets", sheets);
		}
		return mv;
	}

	// API to save files in database
	@PostMapping("/uploadSheet")
	public ModelAndView uploadMultipleFiles(@RequestParam("files") MultipartFile[] files, ModelAndView mv, Model m,
			@ModelAttribute("user") User user,
			@ModelAttribute("attendanceSheetBean") AttendanceSheetBean attendanceSheetBean,
			HttpServletRequest request) {
		int userIdfk = (int) request.getSession().getAttribute("userId");
		user.setUserid(userIdfk);
		for (MultipartFile file : files) {
			attendanceSheetService.saveFile(file, attendanceSheetBean, user);
		}
		List<AttendanceSheet> sheets = attendanceSheetService.getSheetByuserIdFk(userIdfk);
		String message = "Document uploaded Sucesfully";
		mv = new ModelAndView("AttendanceSheet");
		mv.addObject("message", message);
		m.addAttribute("sheets", sheets);
		return mv;
	}

	// API to download files that exist in database
		@GetMapping("/downloadSheet/{fileId}")
		public ResponseEntity<ByteArrayResource> downloadFile(@PathVariable Integer fileId){
			AttendanceSheet sheet = attendanceSheetService.getSheets(fileId).get();
			return ResponseEntity.ok()
					.contentType(MediaType.parseMediaType(sheet.getSheetType()))
					.header(HttpHeaders.CONTENT_DISPOSITION,"attachment:filename=\""+sheet.getSheetName()+"\"")
					.body(new ByteArrayResource(sheet.getSheet()));
		}
		
		// method to delete document
			@RequestMapping(value = "/deleteSheet/{sheetId}")
			public ModelAndView deleteDoc(@PathVariable(name = "sheetId") int sheetId, ModelAndView mv, Model m,HttpServletRequest request) throws IOException {
				String message ="Document Deleted Successfully";
				attendanceSheetService.deleteAttendanceSheet(sheetId);
				int userIdfk=(int) request.getSession().getAttribute("userId");
				List<AttendanceSheet> sheets = attendanceSheetService.getSheetByuserIdFk(userIdfk);
				mv = new ModelAndView("AttendanceSheet");
				mv.addObject("message1", message);
				m.addAttribute("sheets", sheets);
				return mv;
			}
}