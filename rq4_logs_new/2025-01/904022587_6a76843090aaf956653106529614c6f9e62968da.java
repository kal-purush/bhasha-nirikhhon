package com.green.community.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.green.community.mapper.CommunityMapper;
import com.green.community.vo.CommunityVo;

import jakarta.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/Community")
public class CommunityController {
	
	@Value("${part4.upload-path}")
	private String uploadPath;
	
	@Autowired
	private CommunityMapper communityMapper;

	//------------------------커뮤니티 메인화면-----------------------------//
	// Community/Albumlist (홈)
	@RequestMapping("/Albumlist")
	public ModelAndView albumlist(
			@RequestParam(value = "page", defaultValue = "1") int currentPage,
			@RequestParam(value = "sort", defaultValue = "등록순") String sort,
			@RequestParam(value = "search", defaultValue = "제목") String search,
			@RequestParam(value = "searchtext", defaultValue = "") String searchtext) {
	    // Determine the sort order
	    String orderBy = "COMMUNITY_REGDATE DESC";
	    if ("조회순".equals(sort)) {
	        orderBy = "COMMUNITY_VIEWS DESC";
	    }
	    
	    // Determine the search order
	    String searchBy = "C.COMMUNITY_TITLE";
	    if ("작성자".equals(search)) {
	    	searchBy = "USER_NICKNAME";
	    }
	    
	    //페이지네이션
	    int itemsPerPage = 12; // 페이지당 항목 수
	    int totalItems = communityMapper.getTotalAlbumCount(searchBy, searchtext); // 총 앨범 수를 가져오는 메서드
	    int totalPages = (int) Math.ceil((double) totalItems / itemsPerPage); // 총 페이지 수 계산
	    int startIndex = (currentPage - 1) * itemsPerPage; // 시작 인덱스 계산

		// 조립앨범 List
		List<CommunityVo> albumList = communityMapper.getalbumList(startIndex, itemsPerPage, orderBy, searchBy, searchtext);
		
		// 조립앨범 BEST List
		List<CommunityVo> albumbestList = communityMapper.getalbumbestList();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("albumList", albumList);
		mv.addObject("albumbestList", albumbestList);
		mv.addObject("totalPages", totalPages);
		mv.addObject("currentPage", currentPage);
		mv.addObject("orderBy", orderBy);
		mv.addObject("searchBy", searchBy);
		mv.addObject("searchtext", searchtext);
		mv.setViewName("community/albumlist");
		return mv;
	}
	//------------------------커뮤니티 조립앨범 게시글 작성-----------------------------//
	// Community/AlbumwriteForm (게시글 작성)
	@RequestMapping("/AlbumwriteForm")
	public ModelAndView albumwriteForm(CommunityVo communityVo, @RequestParam("user_idx") int user_idx) {
		int cpuNum = 5;
		int mainboardNum = 6;
		int memoryNum = 7;
		int gpuNum = 8;
		int ssdNum = 9;
		int hddNum = 10;
		int coolerNum = 11;
		int caseNum = 12;
		int powerNum = 13;
		
		List<String> cpuList = communityMapper.getproductList(cpuNum);
		List<String> mainboardList = communityMapper.getproductList(mainboardNum);
		List<String> memoryList = communityMapper.getproductList(memoryNum);
		List<String> gpuList = communityMapper.getproductList(gpuNum);
		List<String> ssdList = communityMapper.getproductList(ssdNum);
		List<String> hddList = communityMapper.getproductList(hddNum);
		List<String> coolerList = communityMapper.getproductList(coolerNum);
		List<String> caseList = communityMapper.getproductList(caseNum);
		List<String> powerList = communityMapper.getproductList(powerNum);
		
	    // 조립앨범 BEST List
	 	List<CommunityVo> albumbestList = communityMapper.getalbumbestList();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("communityVo", communityVo);
		mv.addObject("user_idx", user_idx);
		mv.addObject("cpuList", cpuList);
		mv.addObject("mainboardList", mainboardList);
		mv.addObject("memoryList", memoryList);
		mv.addObject("gpuList", gpuList);
		mv.addObject("ssdList", ssdList);
		mv.addObject("hddList", hddList);
		mv.addObject("coolerList", coolerList);
		mv.addObject("caseList", caseList);
		mv.addObject("powerList", powerList);
		mv.addObject("albumbestList", albumbestList);
		mv.setViewName("community/albumwrite");
		return mv;
		}
	
	// Community/Albumwrite (게시글 작성)
	@RequestMapping(value = "/Albumwrite", method = RequestMethod.POST)
	public ModelAndView albumwrite(
	        @ModelAttribute CommunityVo communityVo,
	        @RequestParam("user_idx") int user_idx,
	        @RequestParam("community_image_name") MultipartFile[] files,
	        HttpServletRequest request) {
		// 문의글 저장
		communityMapper.insertcommunity(communityVo);

		// 업로드 경로 설정
	    HashMap<String, Object> map = new HashMap<>();
	    map.put("uploadPath", uploadPath);
	    map.put("communityProfile", "communityProfile");
	    System.out.println("map: " + uploadPath);
	    // 파일 저장 후 경로와 파일명 저장
	    FileImage.save(map, files);
	    
	    // 사진 저장
	    communityMapper.insertcommunityImage(map);

	    ModelAndView mv = new ModelAndView();
		mv.addObject("user_idx", user_idx);
		mv.addObject("uploadPath", uploadPath);
	    mv.setViewName("redirect:/Community/Albumlist");
	    return mv;
	}
	
	// Community/Albumview (게시글 상세보기)
	@RequestMapping("/Albumview")
	public ModelAndView albumview(
			@RequestParam("community_idx") int community_idx,
			@RequestParam("user_idx") int user_idx
			) {
	    // 게시글 정보
		CommunityVo vo = communityMapper.getinfo(community_idx, user_idx);
		
		// 스펙 정보
		CommunityVo spec = communityMapper.getspecinfo(community_idx);
		
		// 댓글 List
		List<CommunityVo> answerList = communityMapper.getanswerList(community_idx);
		
		// 조립앨범 BEST List
		List<CommunityVo> albumbestList = communityMapper.getalbumbestList();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("community_idx", community_idx);
		mv.addObject("user_idx", user_idx);
		mv.addObject("vo", vo);
		mv.addObject("spec", spec);
		mv.addObject("answerList", answerList);
		mv.addObject("albumbestList", albumbestList);
	    mv.setViewName("community/albumview");
	    return mv;
	}
	// Community/Answerwrite (게시글 댓글 등록)
	@RequestMapping("/Answerwrite")
	public ModelAndView answerwrite(
			CommunityVo communityVo,
			@RequestParam("community_idx") int community_idx,
			@RequestParam("user_idx") int user_idx
			) {
		// 댓글 저장
		communityMapper.insertanswer(communityVo);
				
		ModelAndView mv = new ModelAndView();
		mv.addObject("community_idx", community_idx);
		mv.addObject("user_idx", user_idx);
		mv.setViewName("redirect:/Community/Albumview");
		return mv;
	}
	
	//------------------------커뮤니티 조립앨범 게시글 수정-----------------------------//
	// Community/AlbumupdateForm (게시글 수정)
	@RequestMapping("/AlbumupdateForm")
	public ModelAndView albumupdateForm(
			CommunityVo communityVo, 
			@RequestParam("community_idx") int community_idx) {
		int cpuNum = 5;
		int mainboardNum = 6;
		int memoryNum = 7;
		int gpuNum = 8;
		int ssdNum = 9;
		int hddNum = 10;
		int coolerNum = 11;
		int caseNum = 12;
		int powerNum = 13;
		
		List<String> cpuList = communityMapper.getproductList(cpuNum);
		List<String> mainboardList = communityMapper.getproductList(mainboardNum);
		List<String> memoryList = communityMapper.getproductList(memoryNum);
		List<String> gpuList = communityMapper.getproductList(gpuNum);
		List<String> ssdList = communityMapper.getproductList(ssdNum);
		List<String> hddList = communityMapper.getproductList(hddNum);
		List<String> coolerList = communityMapper.getproductList(coolerNum);
		List<String> caseList = communityMapper.getproductList(caseNum);
		List<String> powerList = communityMapper.getproductList(powerNum);
		
		// 게시글 정보
		CommunityVo vo = communityMapper.getinfo(community_idx);
		
	    // 조립앨범 BEST List
	 	List<CommunityVo> albumbestList = communityMapper.getalbumbestList();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("vo", vo);
		mv.addObject("community_idx", community_idx);
		mv.addObject("cpuList", cpuList);
		mv.addObject("mainboardList", mainboardList);
		mv.addObject("memoryList", memoryList);
		mv.addObject("gpuList", gpuList);
		mv.addObject("ssdList", ssdList);
		mv.addObject("hddList", hddList);
		mv.addObject("coolerList", coolerList);
		mv.addObject("caseList", caseList);
		mv.addObject("powerList", powerList);
		mv.addObject("albumbestList", albumbestList);
		mv.setViewName("community/albumupdate");
		return mv;
		} 
	// Community/Albumupdate (게시글 수정)
	@RequestMapping(value = "/Albumupdate", method = RequestMethod.POST)
	public ModelAndView albumupdate(
	        @ModelAttribute CommunityVo communityVo,
	        @RequestParam("community_idx") int community_idx,
	        @RequestParam("community_image_name") MultipartFile[] files,
	        HttpServletRequest request) {
		
		// 문의글 수정
		communityMapper.updatecommunity(communityVo);

		// 파일이 있는 경우에만 업로드 경로 설정 및 파일 저장
	    if (files != null && files.length > 0) {
	        // 실제 파일이 비어있지 않은 경우만 처리
	        boolean hasValidFile = false;
	        for (MultipartFile file : files) {
	            if (!file.isEmpty()) {
	                hasValidFile = true;
	                break;
	            }
	        }

	        if (hasValidFile) {
	            // 업로드 경로 설정
	            HashMap<String, Object> map = new HashMap<>();
	            map.put("uploadPath", uploadPath);
	            map.put("communityProfile", "communityProfile");
	            System.out.println("map: " + uploadPath);

	            // 파일 저장 후 경로와 파일명 저장
	            FileImage.save(map, files);

	            // 사진 수정
	            communityMapper.updatecommunityImage(map, community_idx); 
	        }
	    }

	    ModelAndView mv = new ModelAndView();
		mv.addObject("community_idx", community_idx);
		mv.addObject("uploadPath", uploadPath);
	    mv.setViewName("redirect:/Community/Albumlist");
	    return mv;
	}
	//------------------------커뮤니티 조립앨범 게시글 삭제-----------------------------//
	// Community/Albumdelete (게시글 삭제)
	@RequestMapping("/Albumdelete")
	public ModelAndView albumdelete(
			CommunityVo communityVo, 
			@RequestParam("community_idx") int community_idx) {
		// 게시글 삭제
		communityMapper.deleteimage(communityVo); 
		communityMapper.deletealbum(communityVo); 
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("community_idx", community_idx);
	    mv.setViewName("redirect:/Community/Albumlist");
	    return mv;
	}
}