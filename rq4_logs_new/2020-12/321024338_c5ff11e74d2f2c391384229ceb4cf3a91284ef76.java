package rw.member.controller;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.oreilly.servlet.MultipartRequest;
import com.oreilly.servlet.multipart.DefaultFileRenamePolicy;

import rw.member.model.service.MemberService;
import rw.member.model.vo.Member;

/**
 * Servlet implementation class ProfileUploadServlet
 */
@WebServlet("/profileUpload.rw")
public class ProfileUploadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ProfileUploadServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String uploadPath = "/resources/file/";
		ServletContext context = request.getServletContext();
		String realUploadPath = context.getRealPath(uploadPath);
		int uploadFileSizeLimit = 10 * 1024 * 1024; // 10MB
		String encType="UTF-8";
		
		MultipartRequest multi = new MultipartRequest(request,realUploadPath,
				uploadFileSizeLimit,encType,new DefaultFileRenamePolicy());
		
		String originalFileName = multi.getFilesystemName("file");
		
		HttpSession session = request.getSession();
		Member m = (Member)session.getAttribute("member");
		String fileUser = m.getMemberId();
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 포맷 만들기
		Date currentTime = Calendar.getInstance().getTime();
		Timestamp uploadTime = Timestamp.valueOf(formatter.format(currentTime));
		System.out.println(uploadTime.toString());
		
		File file = new File(realUploadPath+"\\"+"originalFileName");
		file.renameTo(new File(realUploadPath+"\\"+currentTime+"_rw"));
		String changedFileName = currentTime+"_rw";
		
		File reNameFile = new File(realUploadPath+"\\"+changedFileName);
		String filePath = reNameFile.getPath();
		
		m.setMemberId(fileUser);
		m.setProfileImg(changedFileName);
		
		int result = new MemberService().uploadFile(m);
		
		response.setCharacterEncoding("UTF-8");
		PrintWriter out = response.getWriter();
		if(result > 0) {
			out.print("<script>alert('프로필 사진이 변경되었습니다.');</script>");
		} else {
			out.print("<script>alert('프로필 사진 변경이 정상적으로 처리되지 못헀습니다. 다시 변경해주세요.');</script>");
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}