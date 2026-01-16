package project2.scmaster.rodo.controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import project2.scmaster.rodo.dao.Rodo_QaBoardDao;
import project2.scmaster.rodo.util.FileService;
import project2.scmaster.rodo.util.PageNavigator;
import project2.scmaster.rodo.vo.Rodo_FreeBoard;
import project2.scmaster.rodo.vo.Rodo_QaBoard;
import project2.scmaster.rodo.vo.Rodo_QaReply;

@Controller
public class QaBoardController 
{	
	List<Integer> reList;
	List<Integer> fList;
	
	// 서버 리부팅 이전 삭제 카운트 설정
	int num = 12;
	
	final int countPerPage = 15;		// 페이지 당 글 수
	final int pagePerGroup = 5;		// 페이지 이동 그룹 당 표시할 페이지 수
	
	@Autowired
	Rodo_QaBoardDao dao;
	
	final String uploadPath = "/boardfile";
	
	@RequestMapping(value = "qaboardlist", method = RequestMethod.GET)
	public String qaboardlist(Model model,
			@RequestParam(value = "page", defaultValue = "1") int page,
			@RequestParam(value = "searchText", defaultValue = "") String searchText
			)
	{
		int total = dao.listsize(searchText);
		
		System.out.println("Q & A 게시글 수 : "+total);
		
		PageNavigator navi = new PageNavigator(countPerPage, pagePerGroup, page, total);
		
		List<Rodo_QaBoard> list = dao.list(navi.getStartRecord(), navi.getCountPerPage(), searchText);
		
		// 리플 카운트 List 선언 (초기화)
		reList = new ArrayList<> ();
		
		// 게시판 사이즈 + 글 삭제 했을 시 사이즈에 추가 할 변수
		int rSize = list.size() + num;	
		
		// 게시판 사이즈 + 삭제한 카운트 만큼 반복
		for (int i = 0; i < rSize; i++)
		{
			// 해당 글 번호 리플 수를 Get!
			int temp = dao.getReplyCount(i);
		
			// 해당 글에 리플이 없는 경우 (해당 리스트 인덱스에 null 대입)
			if (temp == 0 || temp == -1)
			{
				reList.add(i, null);
			}
			
			// 해당 글에 리플이 있는 경우 (해당 리스트 인덱스에 가져 온 리플 카운트 대입)
			else if (temp > 0)
			{
				reList.add(i, temp);
			}
		}
		
		// 첨부 파일 카운트 List 선언 (초기화)
		fList = new ArrayList<> ();
		
		// 파일 리스트 사이즈 + 글 삭제 했을 시 사이즈에 추가 할 변수
		// (이하 위와 동일)
		for (int j = 0; j < rSize; j++)
		{
			int temp2 = dao.getFileCount(j);
			
			if (temp2 == 0 || temp2 == -1)
			{
				fList.add(j, null);
			}
			
			else if (temp2 > 0)
			{
				fList.add(j, temp2);
			}
		}
		
		model.addAttribute("navi", navi);
		model.addAttribute("searchText", searchText);
		
		// 게시 글 (모델)
		model.addAttribute("list", list);
		
		// 리플 카운트 (모델)
		model.addAttribute("reList", reList);
		
		// 파일 카운트 (모델)
		model.addAttribute("fList", fList);
		
		return "qaboard/qaboardlist";
	}
	
	@RequestMapping(value = "qaboardwrite", method = RequestMethod.GET)
	public String qaboardwrite()
	{
		return "qaboard/qaboardwrite";
	}
	
	@RequestMapping (value = "qaboardwrite", method = RequestMethod.POST)
	public String qaboardwrite (Rodo_QaBoard board, HttpSession session, Model model, 
			MultipartHttpServletRequest request, HttpServletResponse response)
	{
		String id = (String)session.getAttribute("loginId");
		board.setQa_id(id);
		
		Iterator<String> itr = request.getFileNames();
		
		String OriginalFilename = ""; // 내가 저장할 파일의 이름.(hi.jpg)
		String Serversavedfile = ""; 	// 서버에 저장할 날짜로 이름이 바뀌어 저장되는
		
		ArrayList<String> originalFile = new ArrayList<>(); // vo에 담을 OriginalFile
		ArrayList<String> savedFile = new ArrayList<>(); //	vo에 담을 SavedFile
		
		while (itr.hasNext())
		{ //만약 파일이 있다면
			MultipartFile mpf = request.getFile(itr.next()); // 리퀘스트에서 파일을 하나씩 가져옴.
			System.out.println("유저가 저장한 원본파일이름 : " + mpf.getOriginalFilename() + " uploaded!");
			if (!mpf.isEmpty())
			{ // 역시 파일이 있다면.
				Serversavedfile = FileService.saveFile(mpf, uploadPath); //파일을 업로드패쓰의 경로에 저장.
				// 서버폴더에 저장하는 파일(바뀐 이름)을 보내서 섬네일파일의 이름을 가져옴.
				// 확장자가 없는 섬네일의 이름임.
				//포토보드의 vo에 저장할 어레이리스트 세팅.
				originalFile.add(mpf.getOriginalFilename());
				savedFile.add(Serversavedfile);
			}
		}
		System.out.println("savedfile"+savedFile);
		board.setQafile_original(originalFile);
		board.setQafile_saved(savedFile);
		
		int result = dao.write(board);
		int boardNum = 0;
		
		if (result == 1)
		{
			boardNum = dao.getsequence();
			board.setQa_boardnum(boardNum);
			int a = dao.writefile(board);
			
			if(a != 0)
			{
				return "redirect:/qaboardlist";
			}
		}
		return "qaboard/qaboardwrite";
	}
	
	@RequestMapping(value = "qaboardread", method = RequestMethod.GET)
	public String qaboardread(int qa_boardnum, Model model, HttpSession session)
	{
		ArrayList<String> oglist = new ArrayList<>();
		ArrayList<String> svlist = new ArrayList<>();
		
		Rodo_QaBoard board = dao.selectOne(qa_boardnum);
		dao.hitsPlus(qa_boardnum);
		ArrayList<HashMap> fileList = dao.QaFileList(qa_boardnum);
		
		for(HashMap file : fileList)
		{
			System.out.println((String)file.get("QAFILE_SAVED"));
			oglist.add((String)file.get("QAFILE_ORIGINAL"));
			svlist.add((String)file.get("QAFILE_SAVED"));
		}
		System.out.println(oglist);
		System.out.println(svlist);
		
		board.setQafile_original(oglist);
		board.setQafile_saved(svlist);
		
		List<Rodo_QaReply> replylist = dao.findreply(qa_boardnum);
		
		model.addAttribute("board", board);
		model.addAttribute("id", (String)session.getAttribute("loginId"));
		model.addAttribute("replylist", replylist);
			
		return "qaboard/qaboardread";
	}
	
	@RequestMapping(value = "deleteqaboard", method = RequestMethod.GET)
	public String deleteqaboard(int qa_boardnum, HttpSession session)
	{
		ArrayList<HashMap> fileList = dao.QaFileList(qa_boardnum);
		
		ArrayList<String> oglist = new ArrayList<>();
		ArrayList<String> svlist = new ArrayList<>();
		
		for(HashMap file : fileList)
		{
			oglist.add((String)file.get("QAFILE_ORIGINAL"));
			svlist.add((String)file.get("QAFILE_SAVED"));
		}
		
		for(int i = 0; i<oglist.size(); i++)
		{
			FileService.deleteFile(oglist.get(i));
			FileService.deleteFile(svlist.get(i));
		}
		
		List<Rodo_QaReply> list = dao.findreply(qa_boardnum);
		
		for (int i=0; i<list.size(); i++)
		{
			if (list.get(i) != null)
			{
				dao.deletereply(list.get(i));
			}
		}
		dao.delete(qa_boardnum);
		return "redirect:/qaboardlist";
	}
	
	@RequestMapping(value = "updateqaboard", method = RequestMethod.GET)
	public String updateqaboard(int qa_boardnum, Model model)
	{
		Rodo_QaBoard board = dao.selectOne(qa_boardnum);
		
		if (board != null)
		{
			model.addAttribute("board", board);
			
			return "qaboard/qaupdate";
		}
		
		return "qaboard/qaupdate";
	}
	/*
	@ResponseBody
	@RequestMapping(value = "updateqaboard", method = RequestMethod.POST)
	public String updateqaboard
	(Rodo_QaBoard board, MultipartHttpServletRequest request, HttpServletResponse response, int qa_boardnum)
	{	
		Iterator<String> itr = request.getFileNames();
		System.out.println(board.toString() + "exam"); 
		
		String OriginalFilename = ""; // 내가 저장할 파일의 이름.(hi.jpg)
		String Serversavedfile = ""; // 서버에 저장할 날짜로 이름이 바뀌어 저장되는
										// 파일.(170329//////////////////)
		
		ArrayList<String> originalFile = new ArrayList<>(); // vo에 담을 OriginalFile
		ArrayList<String> savedFile = new ArrayList<>(); // vo에 담을 SavedFile
		
		while (itr.hasNext())
		{ //만약 파일이 있다면
			MultipartFile mpf = request.getFile(itr.next()); // 리퀘스트에서 파일을 하나씩 가져옴.
			System.out.println("유저가 저장한 원본 파일 이름 : " + mpf.getOriginalFilename() + " uploaded!");

			if (!mpf.isEmpty())
			{ // 역시 파일이 있다면.
				Serversavedfile = FileService.saveFile(mpf, uploadPath); //파일을 업로드패쓰의 경로에 저장.
																							//이 때 오늘의 등록날짜시간으로 저장.
				// 서버폴더에 저장하는 파일(바뀐 이름)을 보내서 섬네일파일의 이름을 가져옴.
				//포토보드의 vo에 저장할 어레이리스트 세팅.
				originalFile.add(mpf.getOriginalFilename());
				savedFile.add(Serversavedfile);
			}
		}
		board.setQafile_original(originalFile);
		board.setQafile_saved(savedFile);
	
		int i = dao.update(board);
		
		if (i != 0)
		{
			return "success";
		}
		return "fail";
	}		*/
	
	@ResponseBody
	@RequestMapping(value = "updateqaboard", method = RequestMethod.POST)
	public String updateqaboard(Rodo_QaBoard board,
		MultipartHttpServletRequest request, HttpServletResponse response, HttpSession session)
	{	
		int i = 0;
		Iterator<String> itr = request.getFileNames();

//		String id = (String)session.getAttribute("loginId");
//		board.setQa_id(id);
		
		System.out.println(board.toString() + "asdfadfs"); 
		String OriginalFilename = ""; // 내가 저장할 파일의 이름.(hi.jpg)
		String Serversavedfile = ""; // 서버에 저장할 날짜로 이름이 바뀌어 저장되는
										// 파일.(170329//////////////////)
		
		ArrayList<String> originalFile = new ArrayList<>(); // vo에 담을 OriginalFile
		ArrayList<String> savedFile = new ArrayList<>(); // vo에 담을 SavedFile
		
		while (itr.hasNext())
		{ //만약 파일이 있다면
			MultipartFile mpf = request.getFile(itr.next()); // 리퀘스트에서 파일을 하나씩 가져옴.
			System.out.println("유저가 저장한 원본파일이름 : " + mpf.getOriginalFilename() + " uploaded!");
			
			if (!mpf.isEmpty())
			{ // 역시 파일이 있다면.
				Serversavedfile = FileService.saveFile(mpf, uploadPath); //파일을 업로드패쓰의 경로에 저장.
																		//이때 오늘의 등록날짜시간으로 저장.
				// 서버폴더에 저장하는 파일(바뀐 이름)을 보내서 섬네일파일의 이름을 가져옴.
				//포토보드의 vo에 저장할 어레이리스트 세팅.
				originalFile.add(mpf.getOriginalFilename());
				savedFile.add(Serversavedfile);
			}
		}

		board.setQafile_original(originalFile);
		board.setQafile_saved(savedFile);
		
		System.out.println(board.toString() + ".. 업데이트 전");
		
		i = dao.updateqa(board);
		
		 if(i != 0)
		 {
			 return "success";
		 }
		 return "fail";
	}
	
	
	
/*	
 * @RequestMapping(value = "qadownload", method = RequestMethod.GET)
	public String download(int qa_boardnum, HttpServletResponse response)
	{
		Rodo_QaBoard board = dao.selectOne(qa_boardnum);
		
		try 
		{
			response.setHeader("Content-Disposition", "attachment;filename="+URLEncoder.encode(board.getQafile_original(), "UTF-8");
		}
		
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
		
		// 저장된 파일 경로
		String fullPath = uploadPath + "/" + board.getQa_savedfile();
		
		// 서버의 파일을 읽을 입력 스트림
		// 클라이언트에게 전달할 출력 스트림
		FileInputStream filein = null;
		ServletOutputStream fileout = null;
		
		try 
		{
			filein = new FileInputStream(fullPath);
			fileout = response.getOutputStream();
			
			// Spring 파일 관련 유틸
			FileCopyUtils.copy(filein, fileout);
		} 
		
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		return null;
	}
*/
	
	@RequestMapping(value = "qaLoad", method = RequestMethod.GET)
	public void qaLoad(String origin, HttpServletResponse response)	
	{
		try
		{
			response.setHeader("Content-Disposition", " attachment;filename=" + URLEncoder.encode(origin, "UTF-8"));
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		
		String OriginfullPath = uploadPath + "/" + origin; // 원래파일

		FileInputStream filein0 = null;
		ServletOutputStream fileout0 = null;
		
		try {
			filein0 = new FileInputStream(OriginfullPath);
			fileout0 = response.getOutputStream();

			// Spring의 파일 관련 유틸
			FileCopyUtils.copy(filein0, fileout0);

			filein0.close();
			fileout0.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@ResponseBody
	@RequestMapping(value = "qareplywrite", method = RequestMethod.POST)
	public List<Rodo_QaReply> qareplywrite (Rodo_QaReply reply, HttpSession session, Model model)
	{	
		String id = (String)session.getAttribute("loginId");
		reply.setQareply_id(id);
		
		dao.writereply(reply);
		
		List<Rodo_QaReply> list = dao.findreply(reply.getQa_boardnum());
		
		return list;
	}
	
	@ResponseBody
	@RequestMapping(value = "qadeletereply", method = RequestMethod.GET)
	public List<Rodo_QaReply> qadeletereply(Rodo_QaReply reply, HttpSession session, Model model)
	{	
	//	Rodo_QaReply reply = dao.selectreply(Qareply_replynum);
		
		String id = (String)session.getAttribute("loginId");
		reply.setQareply_id(id);
		
		dao.deletereply(reply);
		
		List<Rodo_QaReply> list = dao.findreply(reply.getQa_boardnum());
		
	//	return "redirect:/read?Qa_boardnum="+reply.getQa_boardnum();
		
		return list;
	}
}