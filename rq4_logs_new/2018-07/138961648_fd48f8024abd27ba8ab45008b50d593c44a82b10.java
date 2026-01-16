package com.neusoft.control;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.neusoft.po.Lesson;
import com.neusoft.po.LessonBranch;
import com.neusoft.po.Sorder;
import com.neusoft.po.Swiper;
import com.neusoft.po.Address;
import com.neusoft.po.FreeListen;
import com.neusoft.service.CourseService;
import com.neusoft.service.EnterpriseServiceForCourse;
import com.neusoft.service.OrderServiceForCourse;
import com.neusoft.vo.AddressVo;
import com.neusoft.vo.CourseCategoryVo;
import com.neusoft.vo.CoursePageVo;
import com.neusoft.vo.FreeListenPageVo;
import com.neusoft.vo.TotalCoursePageVo;
import com.neusoft.vo.TotalFreeListenPageVo;


@Controller
public class CourseHandler {
	
	@Autowired
	private CourseService courseService;
	
	@Autowired
	private EnterpriseServiceForCourse branchService;
	
	@Autowired
	private OrderServiceForCourse orderService;
	
	@RequestMapping(value="/upload/cover")
	@ResponseBody		
	public String upload(@RequestParam("file") MultipartFile upload,HttpServletRequest request){
		String original_name=upload.getOriginalFilename();
		String filename = System.currentTimeMillis()+request.getSession().getId()+original_name;

		String path = request.getServletContext().getRealPath("/");
		
		File f = new File(path);
		String ppath = f.getParent();
		
		System.out.println(ppath);

		File dest = new File(ppath+"/upload/cover", filename);
		try {
			upload.transferTo(dest);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String result="{\"code\":0,\"url\":\""+filename+"\",\"name\":\""+original_name+"\"}";
		System.out.println(result);
		return result;
	}
	
	@RequestMapping(value="/BackEnd/upload/lesson")
	public String uploadLesson(HttpServletRequest request) throws Exception
	{
		System.out.println(request.getParameter("l_id"));
		System.out.println(request.getParameter("c_name"));
		System.out.println(request.getParameter("editor"));
		System.out.println(request.getParameter("imgurl"));
		System.out.println(request.getParameter("classification"));
		System.out.println(request.getParameter("lprice"));
		System.out.println(request.getParameter("sub"));
		
		Lesson l=new Lesson();
		
		l.setLname(request.getParameter("c_name"));
		l.setLprice(Float.parseFloat(request.getParameter("lprice")));
		l.setLdesc(request.getParameter("editor"));
		l.setImgUrl(request.getParameter("imgurl"));
		l.setCategory(request.getParameter("classification"));
		
		int qid=branchService.getqidByBranchName(request.getParameter("sub"));
		l.setQid(qid);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		l.setPubTime(df.format(System.currentTimeMillis()));
		if(request.getParameter("l_id")==null) {
			courseService.saveLesson(l);
		}
		else {
			System.out.println("这是修改  开始修改");
			l.setLid(Integer.parseInt(request.getParameter("l_id")));
			courseService.updateLesson(l);
		}
		int bid=branchService.getBranchIdByBranchName(request.getParameter("sub"));
		LessonBranch lb=new LessonBranch();
		lb.setLid(l.getLid());
		lb.setBranchid(bid);
		
		if(request.getParameter("l_id")==null) {
			courseService.saveLessonBranch(lb);
		}
		else {
			System.out.println("这是修改  开始修改");
			courseService.updateLessonBranch(lb);
		}
		
		
		return "redirect:/BackEnd/detail/lesson?lid="+lb.getLid();
	}
	
	@RequestMapping(value="/BackEnd/upload/freelisten")
	public String uploadFreeListen(HttpServletRequest request) throws Exception
	{
		System.out.println(request.getParameter("f_id"));
		System.out.println(request.getParameter("f_title"));
		System.out.println(request.getParameter("editor"));
		System.out.println(request.getParameter("imgurl"));
		System.out.println(request.getParameter("f_status"));
		System.out.println(request.getParameter("sub"));
		
		FreeListen l=new FreeListen();
		
		l.setTitle(request.getParameter("f_title"));
		l.setFdesc(request.getParameter("editor"));
		l.setImgUrl(request.getParameter("imgurl"));
		l.setStatus(request.getParameter("f_status"));
		
	//这个字段被他们删了！！	
	//	int qid=branchService.getqidByBranchName(request.getParameter("sub"));
		int branchid=branchService.getBranchIdByBranchName(request.getParameter("sub"));
		l.setBranchid(branchid);
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		l.setPubTime(df.format(System.currentTimeMillis()));
		
		if(request.getParameter("f_id")==null) {
			courseService.saveFreeListen(l);
		}
		else {
			System.out.println("这是修改  开始修改");
			l.setId(Integer.parseInt(request.getParameter("f_id")));
			courseService.updateFreeListen(l);
		}		
		return "redirect:/BackEnd/detail/freelisten?id="+l.getId();
	}
	
	@RequestMapping(value="/BackEnd/detail/lesson")
	public String detail(HttpServletRequest request) throws Exception
	{
		String lessionId=request.getParameter("lid");
		int lid=Integer.parseInt(lessionId);
		Lesson l=courseService.getLession(lid);
		
		request.setAttribute("lid", l.getLid());
		request.setAttribute("lname", l.getLname());
		request.setAttribute("cover", l.getImgUrl());
		System.out.println(l.getImgUrl()+"*************");
		request.setAttribute("lprice", l.getLprice());
		request.setAttribute("ldesc", l.getLdesc());
		request.setAttribute("category", l.getCategory());
		System.out.println(l.getLdesc()+"-------------------");
		String branchName=branchService.getBranchNameBylid(l.getLid());
		
		request.setAttribute("branchName",branchName );
		
		return "forward:/BackEnd_final/BackEnd_final/courseDetail.jsp";
		
	}

	@RequestMapping(value="/BackEnd/detail/freelisten")
	public String freelistdetail(HttpServletRequest request) throws Exception
	{
		String ID=request.getParameter("id");
		int id=Integer.parseInt(ID);
		FreeListen l=courseService.getFreeListen(id);
		
		request.setAttribute("id", l.getId());
		request.setAttribute("title", l.getTitle());
		request.setAttribute("cover", l.getImgUrl());

		request.setAttribute("fdesc", l.getFdesc());
		request.setAttribute("status", l.getStatus());
		
		String branchName=branchService.getBranchNameByBranchID(l.getBranchid());
		request.setAttribute("branchName",branchName );
		
		return "forward:/BackEnd_final/BackEnd_final/reservationDetail.jsp";
		
	}
		
		@RequestMapping(value="/BackEnd/edit")
		public String editLesson(HttpServletRequest request) throws Exception
		{
			String lessionId=request.getParameter("lid");
			int lid=Integer.parseInt(lessionId);
			Lesson l=courseService.getLession(lid);
			request.setAttribute("lid", l.getLid());
			request.setAttribute("lname", l.getLname());
			request.setAttribute("cover", l.getImgUrl());
			request.setAttribute("lprice", l.getLprice());
			request.setAttribute("ldesc", l.getLdesc());
			request.setAttribute("category", l.getCategory());
			
			int branch=branchService.getBranchidBylid(l.getLid());
			request.setAttribute("branch", "branch"+branch);
			
			/*
			 * 一定要记得把下面一行改成session中的qid
			 */
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			List<Address> branches=branchService.selectAllAddress(qid);
			List<AddressVo> result =new ArrayList<>();
			int i=0;
			for(Address address:branches) {
				//为了方便，我们把address的id稍作修改，作为jsp页面的branch单选框的ID
				result.add(new AddressVo(address.getBranch(),"branch"+address.getAid()));
				System.out.println(result.get(i++));
			}
			request.setAttribute("branches", result);
			
			
			return "forward:/BackEnd_final/BackEnd_final/courseAdd.jsp";
		}
	
		@RequestMapping(value="/BackEnd/editfree")
		public String editFreeListen(HttpServletRequest request) throws Exception
		{
			String ID=request.getParameter("id");
			int id=Integer.parseInt(ID);
			FreeListen l=courseService.getFreeListen(id);
			
			request.setAttribute("id", l.getId());
			request.setAttribute("title", l.getTitle());
			request.setAttribute("cover", l.getImgUrl());

			request.setAttribute("fdesc", l.getFdesc());
			request.setAttribute("status", l.getStatus());
			System.out.println(l.getFdesc()+"-------------------------------------------");
			request.setAttribute("branch", "branch"+l.getBranchid());
			
			/*
			 * 一定要记得把下面一行改成session中的qid
			 */
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			List<Address> branches=branchService.selectAllAddress(qid);
			List<AddressVo> result =new ArrayList<>();
			int i=0;
			for(Address address:branches) {
				//为了方便，我们把address的id稍作修改，作为jsp页面的branch单选框的ID
				result.add(new AddressVo(address.getBranch(),"branch"+address.getAid()));
				System.out.println(result.get(i++));
			}
			request.setAttribute("branches", result);
			
			
			return "forward:/BackEnd_final/BackEnd_final/reservationAdd.jsp";
		}
		
		/*
		 * 上传/修改【收费课程】初始化部门
		 */
		@RequestMapping(value="/BackEnd/initBranch")
		public String initBranchInfo(HttpServletRequest request) throws Exception
		{
			/**
			 * 记得删除！！！
			 */
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			//request.getSession().setAttribute("qid", "1");
			System.out.println("开始初始化页面的部门");
			List<Address> branches=branchService.selectAllAddress(qid);
			List<AddressVo> result =new ArrayList<>();
			for(Address address:branches) {
				//为了方便，我们把address的id稍作修改，作为jsp页面的branch单选框的ID
				result.add(new AddressVo(address.getBranch(),"branch"+address.getAid()));
			}
			request.setAttribute("branches", result);
			System.out.println("我要跳转！！");
			return "forward:/BackEnd_final/BackEnd_final/courseAdd.jsp";
		}
		
		/*
		 * 上传/修改【免费预约课】页面的时候需要获得  有哪些分部？
		 * 
		 */
		@RequestMapping(value="/BackEnd/initBranchFree")
		public String initBranchInfoFreeListen(HttpServletRequest request) throws Exception
		{
			/**
			 * 记得删除！！！
			 */
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			//request.getSession().setAttribute("qid", "1");
			System.out.println("开始初始化页面的部门");
			List<Address> branches=branchService.selectAllAddress(qid);
			List<AddressVo> result =new ArrayList<>();
			for(Address address:branches) {
				//为了方便，我们把address的id稍作修改，作为jsp页面的branch单选框的ID
				result.add(new AddressVo(address.getBranch(),"branch"+address.getAid()));
			}
			request.setAttribute("branches", result);
			System.out.println("我要跳转！！");
			return "forward:/BackEnd_final/BackEnd_final/reservationAdd.jsp";
		}
		
		/*
		 * 课程详情页面，获得课程大部分信息
		 */
		@RequestMapping(value="FrontEnd/getCoursePosition")
		@ResponseBody		
		public String loadCourseDetail(HttpServletRequest request){
			int lid=Integer.parseInt(request.getParameter("lid"));
			
			Lesson l=null;
			
			Address adr = null;
			try {
				 l=courseService.getLession(lid);
				 adr=branchService.getAddressBylid(lid);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(l.getLdesc().replace("\"", "\\\""));
			String result="{\"code\":\"0\","
					+ "\"detail\":\""+l.getLdesc().replace("\"", "\\\"")+"\","
					+ "\"longitude\":\""+adr.getLongitude()+"\","
					+ "\"address\":\""+adr.getAddress()+"\","
					+ "\"phone\":\""+adr.getTel()+"\","
					+ "\"latitude\":\""+adr.getLatitude()+"\"}";
			
			System.out.println(result);
			System.out.println(result);
			return result;
		}
		
		@RequestMapping(value="FrontEnd/getLesson111")
		@ResponseBody		
		public List<String> getPrice(HttpServletRequest request){

			System.out.println("开始获得课程信息");
			Lesson l = null;
			int lid=Integer.parseInt(request.getParameter("lid"));
			try {
				l = courseService.getLession(lid);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		
			List<String> result = new ArrayList<String>();
			result.add(""+l.getLprice());
			result.add(l.getLname());
			return result;

			
		}
	
		
		@RequestMapping(value="/FrontEnd/enterprise/getAllAddress")
		@ResponseBody
		public List<Address> getAllAddress(HttpServletRequest request) throws Exception{
			System.out.println("开始获取企业分部地址啦啦啦");
			//int qid=1;
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
	//		 qid=request.getSession().getAttribute("qid");
			List<Address> addr= branchService.selectAllAddress(qid);
			
		
			return addr;
		}
		
		@RequestMapping(value="FrontEnd/getSwipersB")
		@ResponseBody
		public List<Swiper> getSwipers(HttpServletRequest request) throws Exception{
			//int qid=1;
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			//		 qid=request.getSession().getAttribute("qid");
	
			Swiper s=new Swiper();
			s.setCategory("A");
			s.setQid(qid);
					
			List<Swiper> swipers=branchService.getSwipers(s);
			System.out.println(swipers.size()+"///////////////////");
			
	
			return swipers;
		}
		
		/*
		 * 
		 */
		@RequestMapping(value="FrontEnd/getAllCourseByBranch")
		@ResponseBody		
		public String loadCourseByBranch(HttpServletRequest request){
			//int qid=1;
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
//			qid=request.getSession().getAttribute("qid");
			int aid=Integer.parseInt(request.getParameter("aid"));
			int from=Integer.parseInt(request.getParameter("from"));
			int to=Integer.parseInt(request.getParameter("to"));
			System.out.println(aid+"*********************");
			CoursePageVo cpv = null;
			TotalCoursePageVo tpv = null;
			if(aid!=0) {
				cpv=new CoursePageVo();
				cpv.setAid(aid);
				cpv.setFrom(from);
				cpv.setLimit(to);
			}
			else {
				tpv=new TotalCoursePageVo();
				tpv.setQid(qid);
				tpv.setFrom(from);
				tpv.setTo(to);
			}
		
			List<Lesson> newlist=null;
			int total=0;
			if(aid!=0) {
				try {
					newlist=courseService.selectBranchLessons(cpv);
					total=courseService.selectTotalNumOfBranchLessons(aid);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				try {
					newlist=courseService.selectAllLessons(tpv);
					 total=courseService.selectTotalNumOfLessons(qid);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			String result="{\"total\":"+total+",\"result\":[";
			
			for(Lesson l:newlist) {
				result+="{\"lid\":"+l.getLid()+",";
				result+="\"lname\":\""+l.getLname()+"\",";
				result+="\"url\":\""+l.getImgUrl()+"\"}";
				if(newlist.indexOf(l)<newlist.size()-1)
					result+=",";
				else
					result+="]";
			}
			if(newlist.size()==0) {
				result+="]";
			}
			result+="}";
	
			
			System.out.println(result);
			return result;
		}
		
		

		@RequestMapping(value="FrontEnd/getFreeListenList")
		@ResponseBody		
		public List<FreeListen> getMoreFreeListenForCourseList(HttpServletRequest request) throws Exception{
			//int qid=1;
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
//			qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
			int from=Integer.parseInt(request.getParameter("from"));
			int count=Integer.parseInt(request.getParameter("count"));
			
			List<FreeListen> ll=null;
				TotalFreeListenPageVo fpv=new TotalFreeListenPageVo();
				fpv.setQid(qid);
				fpv.setFrom(from);
				fpv.setTo(count);
				ll= courseService.selectAllFreeListens(fpv);
				System.out.println("看来是选择免费课");
				System.out.println("从第"+from+"到第"+(count+from)+"的课程");

		
			if(ll.size()==0) {
				return new ArrayList<>();
			}


			return ll;
		}
		
		
		
		@RequestMapping(value="FrontEnd/getCourseList")
		@ResponseBody		
		public List<Lesson> getMoreCourseForCourseList(HttpServletRequest request) throws Exception{
			//int qid=1;
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
//			qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
			int code=Integer.parseInt(request.getParameter("code"));
			int from=Integer.parseInt(request.getParameter("from"));
			int count=Integer.parseInt(request.getParameter("count"));
			
			List<Lesson> ll=null;

			 if(code==0) {
				TotalCoursePageVo tpv=new TotalCoursePageVo();
				tpv.setQid(qid);
				tpv.setFrom(from);
				tpv.setTo(count);
				ll=courseService.selectAllLessons(tpv);
				System.out.println("看来是选择所有课");
				System.out.println("从第"+from+"到第"+(count+from)+"的课程");
			}
			else {
				CourseCategoryVo ccv=new CourseCategoryVo();
				ccv.setCount(count);
				ccv.setFrom(from);
				ccv.setQid(qid);
				System.out.println("现在我要选择"+code+"代码的数据");
				System.out.println("从第"+from+"到第"+(count+from)+"的课程");
				switch(code) {
				case 2:
					ccv.setCategory("java");
					break;
				case 3:
					ccv.setCategory("html5");
					break;
				case 4:
					ccv.setCategory("c++");
					break;
				case 5:
					ccv.setCategory("python");
					break;
				}
				System.out.println("看来是按照分类选课");
					ll=courseService.selectLessonsByCategory(ccv);
					
			}
				if(ll.size()==0) {
					return new ArrayList<>();
				}

				for(Lesson l:ll) {
					System.out.println(l.getImgUrl()+"*****");
					
				}
			return ll;
		}
		
		
		@RequestMapping(value="FrontEnd/buy/lesson")
		@ResponseBody		
		public String upload(String username,String phonenumber,int lid,HttpServletRequest request){
			
			HttpSession session = request.getSession();
			int  qid = (int) session.getAttribute("qid");
			Sorder s=new Sorder();
			
			
			s.setLid(lid);
			s.setUsername(username);
			s.setTel(phonenumber);
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String date=df.format(new Date());

			String transactionid=date.toString()+request.getSession().getId().toString();
			s.setQid(qid);
			
			try {
				s.setTotal(courseService.getLession(lid).getLprice());
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			s.setOrderTime(date);
			s.setStatus("待付款");
			s.setTransactionId(transactionid);
			String result=null;
			try {
				orderService.saveOrder(s);
				 result="{\"result\":0}";
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				result="{\"result\":1}";
			}
			return result;			
		}
		@RequestMapping(value="/BackEnd/deleteCourseByLid")
		@ResponseBody
		public String deleteCourseByLid( int lid)throws Exception{
			System.out.println("..........EnterpriseHandler........deleteCourseByLid......");
			
			try {
				courseService.deleteCourse(lid);
				return "{\"result\":true}";
			}catch( Exception e){
				e.printStackTrace();
				return "{\"result\":false}";
			}
			
		}
		
		@RequestMapping(value="/BackEnd/deleteFreeListenById")
		@ResponseBody
		public String deleteFreeListenById( int id)throws Exception{
			System.out.println("..........EnterpriseHandler........deleteCourseByLid......");
			
			try {
				courseService.deleteFreeListen(id);
				return "{\"result\":true}";
			}catch( Exception e){
				e.printStackTrace();
				return "{\"result\":false}";
			}
			
		}
		
		@RequestMapping(value="/BackEnd/selectCourseByQid")
		public String selectCourseByQid(HttpServletRequest request) throws Exception
		{
			/**
			 * 记得删除！！！
			 */
			System.out.println("第一次进入课程列表");
			//int qid=1;
			HttpSession session = request.getSession();
			int qid = (int)session.getAttribute("qid");
	//		qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
		
			List<Address> branches=branchService.selectAllAddress(qid);
			
			
			request.setAttribute("branches", branches);
			request.setAttribute("current_branch", branches.get(0));
			
			CoursePageVo cpv = new CoursePageVo();
			int aid=branches.get(0).getAid();
			cpv.setAid(aid);
			cpv.setFrom((1-1)*5);
			cpv.setLimit(5);
			
			List<Lesson> lessons = courseService.selectBranchLessons(cpv);
			request.setAttribute("lessons", lessons);
			int count = courseService.selectTotalNumOfLessons(qid);
			request.setAttribute("count", count);
			for(Lesson l:lessons) {
				System.out.println("sssss"+l.getLname());
			}
			System.out.println(lessons.size());
			return "forward:/BackEnd_final/BackEnd_final/courseView.jsp";
			
			
		}
		
		@RequestMapping(value="/BackEnd/selectFreeListenByQid")
		public String selectFreeListenByQid(HttpServletRequest request) throws Exception
		{
			/**
			 * 记得删除！！！
			 */
			System.out.println("第一次进入预约课程列表");
			HttpSession session = request.getSession();
			int qid = (int)session.getAttribute("qid");
			//		qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
				
					List<Address> branches=branchService.selectAllAddress(qid);
					
					
					request.setAttribute("branches", branches);
					request.setAttribute("current_branch", branches.get(0));
					
					FreeListenPageVo fpv = new FreeListenPageVo();
					int aid=branches.get(0).getAid();
					fpv.setAid(aid);
					fpv.setFrom((1-1)*5);
					fpv.setLimit(5);
					
					List<FreeListen> freelisten = courseService.selectBranchFreeListens(fpv);
					request.setAttribute("freelisten", freelisten);
					int count = courseService.selectToalNumOfFreeListens(qid);
					request.setAttribute("count", count);
					for(FreeListen f:freelisten) {
						System.out.println("sssss"+f.getTitle());
					}
					return "forward:/BackEnd_final/BackEnd_final/reservationView.jsp";
					
		}
		
		
		@RequestMapping(value="/BackEnd/selectCourseByPage")
		@ResponseBody
		public List<Lesson> selectCourseByPage(int curr, int limit, int branchid,HttpServletRequest request) throws Exception{
			System.out.println(".........EnterpriseHandler..........selectCourseByPage.........");
			HttpSession session = request.getSession();
			int qid = (int)session.getAttribute("qid");
			//测试的时候没有session  整合的时候有  所以到时候去掉注释符号就行
			//		qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
			CoursePageVo cpv = new CoursePageVo();
			
			cpv.setAid(branchid);
			cpv.setFrom((curr-1)*limit);
			cpv.setLimit(limit);
			System.out.println("从第"+curr+"页开始");
			List<Lesson> lessons = courseService.selectBranchLessons(cpv);
			request.setAttribute("lessons", lessons);
			int count = courseService.selectTotalNumOfLessons(qid);
			request.setAttribute("count", count);

			return lessons;
		}
		
		@RequestMapping(value="/BackEnd/selectFreeListenByPage")
		@ResponseBody
		public List<FreeListen> selectFreeListenByPage(int curr, int limit, int branchid,HttpServletRequest request) throws Exception{
			System.out.println(".........EnterpriseHandler..........selectFreeListenByPage.........");
			HttpSession session = request.getSession();
			int qid = (int)session.getAttribute("qid");
			//测试的时候没有session  整合的时候有  所以到时候去掉注释符号就行
			//		qid=Integer.parseInt(request.getSession().getAttribute("qid").toString());
			FreeListenPageVo flpv = new FreeListenPageVo();
		
			flpv.setAid(branchid);
			flpv.setFrom((curr-1)*limit);
			flpv.setLimit(limit);
			System.out.println("从第"+curr+"页开始");
			List<FreeListen> freelistens = courseService.selectBranchFreeListens(flpv);
			request.setAttribute("freelistens", freelistens);
			int count = courseService.selectToalNumOfFreeListens(qid);
			request.setAttribute("count", count);

			return freelistens;
		}
}