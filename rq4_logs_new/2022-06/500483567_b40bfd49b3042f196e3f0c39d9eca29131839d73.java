package chatServer;

public class Protocol {
	//프로토콜의 경우 어플에서 일괄적으로 적용하고 변경될 수 있도록 설계하는 것이 좋을 것이다.
	public static final int WAIT 	    = 100; 
	public static final int ROOM_CREATE = 110;
	public static final int ROOM_LIST	= 120;
	public static final int ROOM_IN 	= 130; 
	public static final int ROOM_INLIST	= 140; 
	public static final int MESSAGE 	= 200; 
	public static final int WHISHER 	= 201; 
	public static final int CHANGE   	= 300; 
	public static final int ROOM_OUT 	= 500; 
	
	//메시지열에서 값에 대한 구분값을 토큰으로 썰어서 사용하므로 이것도 변수로 처리하자.
	public static final String seperator= "#"; 
}