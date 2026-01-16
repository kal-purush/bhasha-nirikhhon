package com.yehan.web.controller;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.LastModifiedFileComparator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.RedirectView;

import com.yehan.web.vo.FileVO;

@RestController
public class FileUploadController {
	
	@Value("${file.download.directory}") 
	private String fileDownloadDirectory;

	// 인덱스 페이지
    @RequestMapping(value = "/",method = RequestMethod.GET)
    public ModelAndView hello(Model model, String year){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("list");
//        if(String.valueOf(year) == null) {
//        	year = "2021";
//        }

        // 파일리스트
        try {
	        File dirFile = new File(fileDownloadDirectory + year);
	        File[] fileList = dirFile.listFiles();
	        Arrays.sort(fileList, LastModifiedFileComparator.LASTMODIFIED_REVERSE);
	        ArrayList<FileVO> dataList = new ArrayList<FileVO>();
	        for(File file: fileList) {
//	            if (file.getName().contains("upload_")) {   // upload_ 문자열이 있는지 체크
	                FileVO fileVO = new FileVO();
	                fileVO.setFilename(file.getName());
//	                fileVO.setName(file.getName().substring(0,3));
	                fileVO.setName("");
	                fileVO.setUrl("upload/" + year + "/");
	                fileVO.setContent("");
	                dataList.add(fileVO);
	                System.out.println("파일정보 : "+file.getName());
//	            }
	        }
	        model.addAttribute("dataList", dataList);
	        model.addAttribute("year", year);
        } catch (Exception e) {
			// TODO: handle exception
        	System.out.println(e.toString());
        	System.out.println(e.getStackTrace());
		}
        return modelAndView;
    }
    
    
    // 사진업로드 화면
    @RequestMapping(value = "/upload",method = RequestMethod.GET)
    public ModelAndView upload(Model model){
        ModelAndView modelAndView = new ModelAndView();
        modelAndView.setViewName("upload");
        return modelAndView;
    }
    // 사진업로드
	@RequestMapping(value = "/upload", method = RequestMethod.POST)
	public RedirectView upload(FileVO filevo) {
		System.out.println(filevo.toString());
		System.out.println("### upload");
		System.out.println(filevo.getFilename());
        String year = "2022";
		int i=0;
		try {
			for(MultipartFile a : filevo.getMultiFiles()) {
				// 확장자 구하기
				String file = a.getOriginalFilename();
					System.out.println("원본이미지명 : " + file);
				String ext = file.substring(a.getOriginalFilename().lastIndexOf(".") + 1);
					System.out.println("확장자명 : " + ext);
				if(i!=0) 
					file = fileDownloadDirectory + year + "/" + filevo.getFilename() + String.valueOf(i) + "." + ext;
				else  
					file = fileDownloadDirectory + year + "/" + filevo.getFilename() + "." + ext;
				
				File targetFile = new File(file);
				InputStream fileStream = a.getInputStream();
				FileUtils.copyInputStreamToFile(fileStream, targetFile);
				
				i++;
			}
		} catch (IOException e) {
//			FileUtils.deleteQuietly(targetFile);
			e.printStackTrace();
		}

		return new RedirectView("/");
	}
	
//	@RequestMapping(value = "/upload", method = RequestMethod.POST)
//	public RedirectView upload(@RequestParam("multiFiles") MultipartFile[] multipartFile, @RequestParam(value ="filename") String filename ) {
//		System.out.println("### upload");
//		System.out.println(filename);
//        String year = "2022";
//		int i=0;
//		try {
//			for(MultipartFile a : multipartFile) {
//				// 확장자 구하기
//				String file = a.getOriginalFilename();
//					System.out.println("원본이미지명 : " + file);
//				String ext = file.substring(a.getOriginalFilename().lastIndexOf(".") + 1);
//					System.out.println("확장자명 : " + ext);
//				if(i!=0) 
//					file = fileDownloadDirectory + year + "/" + filename + String.valueOf(i) + "." + ext;
//				else  
//					file = fileDownloadDirectory + year + "/" + filename + "." + ext;
//				
//				File targetFile = new File(file);
//				InputStream fileStream = a.getInputStream();
//				FileUtils.copyInputStreamToFile(fileStream, targetFile);
//				
//				i++;
//			}
//		} catch (IOException e) {
////			FileUtils.deleteQuietly(targetFile);
//			e.printStackTrace();
//		}
//
//		return new RedirectView("/");
//	}
}
