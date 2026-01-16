package chatClient;


// 로그인 회원가입에 대한 뷰와 dao사이에서의 Controller역할 담당
public class Controller {
	private final String _LOGIN    = "login";
	private final String _IDCHECK  = "idcheck";
	private final String _SIGNUP   = "signup";
	LoginDao logindao = new LoginDao();
	LoginView loginview = null;
	SignUpView signupview = null;

	private Controller() {
	}
	// 생성자 오버로딩(각 뷰에 대해서)
	public Controller(LoginView view) {
		this.loginview = view;
	}
	public Controller(SignUpView view) {
		this.signupview = view;
	}

	public void action(MemberVO pmVO) {
		String command = pmVO.getCommand(); // 반복되는 코드 줄여줌
		MemberVO rsVO = new MemberVO(); 
		// LoginDao 생성하기
		// 로그인
		if(_LOGIN.equals(command)) {
			String nickName = null;
			nickName = logindao.login(pmVO);
			if (nickName.equals("0")) {
				loginview.errorMsg("비밀번호가 틀렸습니다!");
				return;
			} else if (nickName.equals("-1")) {
				loginview.errorMsg("존재하지 않는 아이디입니다.");
				return;
			}else {
				TalkClient tc = new TalkClient(nickName);
				new ChatView(tc);
				tc.init();
				loginview.dispose();
			}
		// 회원가입
		} else if(_SIGNUP.equals(command)) {
			System.out.println("MeberController - 회원가입 호출 성공");
			int result = 0;
			result = logindao.signUp(pmVO);
			if (signupview.spaceCheck()) {
				// 비밀번호 중복검사
				if (signupview.passwordCheck() && result == 1) {
					signupview.successMsg("회원가입을 축하합니다!!");
					new LoginView();
					signupview.dispose();
				} else {
					signupview.errorMsg("입력한 비밀번호가 다릅니다. 확인 해주세요");
					return;
				}
			} else {
				signupview.errorMsg("빈칸이 있습니다. 모두 작성해 주세요.");
				return;
			}
			
		// 아이디 중복체크
		} else if (_IDCHECK.equals(command)) {
			String result = logindao.idCheck(pmVO);
			if (result.equals("-1")) {  // -1이면 사용가능
				signupview.successMsg("입력한 아이디는 사용할 수 있습니다.");
				signupview.jtf_id.setEditable(false); // 사용자가 아이디 다른것 입력하고 싶을 수 있으므로 x
				signupview.jbtn_idcheck.setEnabled(false);
				signupview.isIDCheck = true; // 상수값으로 직접 값변경 x
				signupview.jbtn_ok.setEnabled(signupview.isIDCheck);
				return;
			}else { // 입력아이디 사용 불가(중복있음) = 1이면 사용불가
				signupview.setId("");
				signupview.errorMsg("입력하신 아이디는 이미 존재하는 아이디 입니다.");
				return;
			}
		}
	}

}