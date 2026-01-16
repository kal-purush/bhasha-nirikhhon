package Test;

import static org.junit.Assert.*;

import org.junit.Test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.util.ArrayList;

import main.KeyHandler;
import main.PlayManager;
import main_battle.KeyHandler_2;
import main_battle.PlayManager_Battle;
import mino.Block;



public class MinoTest {
    Block[] b = new Block[4];
    Block[] tempB = new Block[4];
    Color c = Color.red;

    public MinoTest() {
        b[0] = new Block(c);
        b[1] = new Block(c); // 예시로 초기화 값 입력
        b[2] = new Block(c); // 예시로 초기화 값 입력
        b[3] = new Block(c); // 예시로 초기화 값 입력
        
        tempB[0] = new Block(c);
        tempB[1] = new Block(c);
        tempB[2] = new Block(c);
        tempB[3] = new Block(c);
        
    }
	
	protected boolean battle = false;

	
	int autoDropCounter = 0; //오토드랍변수
	public int direction = 1; //방향
	
	//블록 collision 넣어주기 (벽설정)
	public boolean leftCollision, rightCollision, bottomCollision;
	//블록활성화
	public boolean active = true;
	//슬라이딩용
	public boolean deactivating;
	public int deactivateCounter = 0;
	
	//아이템용 예비블록들.
	public Block center;
	public boolean swap = false;
	
	//색깔을 넣어주기.
	//public void create(Color c) //이거 바꾼거.
	@Test
	public void extracted() {
		b[0] = new Block(c);
		b[1] = new Block(c);
		b[2] = new Block(c);
		b[3] = new Block(c);
		
		//temp로 회전모양을 미리 잡아주는 이유는 나중에 모서리부분에서의 collison방지등을 위해서임.
		tempB[0] = new Block(c);
		tempB[1] = new Block(c);
		tempB[2] = new Block(c);
		tempB[3] = new Block(c);
	}
//	@Test
//	public Color create() {
//		int mode = 0;
//		
//		switch(mode) {
//		case 0 :
//			extracted();
//		case 1 :
//			c = (Color.red == c? new Color(213, 94, 0) : (Color.green == c ? new Color(0, 158, 115): c));
//			extracted();
//			
//		case 2 :
//			c = (Color.blue == c? new Color(135, 206, 235) : (Color.yellow == c ? Color.lightGray: c));
//			extracted();
//			
//		}
//		
//		return c;
//	}
//	
	

	@Test
	//돌릴때 쓰는놈.
	public void updateXY() {
		int direction = 0;
		
		//업데이트 여부 체크전 돌아가도되는놈인지 check.
		checkRotationCollision();
		
		//collsion 없을 때에만 돌리기
		leftCollision = false; rightCollision = false;  bottomCollision = false;
		if(leftCollision == false && rightCollision == false && bottomCollision == false) {
			
			this.direction =direction;
			
			for(int i = 0 ; i < 4 ; i++) {		
				b[i].x = tempB[i].x;
				b[i].y = tempB[i].y;
			}
			
			
			
		}
		
		
	}
	
	//override 회전용
	@Test
	public void getDirection1() {}
	@Test
	public void getDirection2() {}
	@Test
	public void getDirection3() {}
	@Test
	public void getDirection4() {}
	
	//collision 체크용
	@Test
	public void checkMovementCollision() {
		
		int leftWall = (battle ? PlayManager_Battle.left_x : PlayManager.left_x);
		int rightWall = (battle ? PlayManager_Battle.right_x : PlayManager.right_x);
		int bottomFloor = (battle ? PlayManager_Battle.bottom_y : PlayManager.bottom_y);
		
		leftCollision = false;
		rightCollision = false;
		bottomCollision = false;
		
		//내가쌓은 블록 check.
		checkStaticBlockCollision();
		
		//아래는프레임 collision 체크과정
		
		//왼쪽벽
		for(int i = 0; i < b.length; i++) { //b.length == 4. b는 4개의 블록이죠?
			if(b[i].x == leftWall) { //4개의 블록들이 left와 붙는지 check
				leftCollision = true;
			}
		}
		
		//오른쪽벽
		for(int i = 0; i < b.length; i++) { //(x,y좌표는 좌측상단꼭지점 그래서 블록크기 더해줌)
			if(b[i].x + Block.SIZE == rightWall) {
				rightCollision = true;
			}
		}
		
		//아래벽
		for(int i = 0; i < b.length; i++) {
			if(b[i].y + Block.SIZE == bottomFloor) {
				bottomCollision = true;
			}
		}
		
	}
	@Test
	public void checkRotationCollision() {
		
		leftCollision = false;
		rightCollision = false;
		bottomCollision = false;
		
		int leftWall = (battle ? PlayManager_Battle.left_x : PlayManager.left_x);
		int rightWall = (battle ? PlayManager_Battle.right_x : PlayManager.right_x);
		int bottomFloor = (battle ? PlayManager_Battle.bottom_y : PlayManager.bottom_y);
		
		//내가 쌓은 블록 check
		checkStaticBlockCollision();
		
		//아래는프레임 collision 체크과정
		
		//왼쪽벽
		for(int i = 0; i < b.length; i++) { //b.length == 4. b는 4개의 블록이죠?
			if(tempB[i].x < leftWall) { //4개의 블록들이 left와 붙는지 check
				leftCollision = true;
			}
		}
		
		//오른쪽벽
		for(int i = 0; i < b.length; i++) { //(x,y좌표는 좌측상단꼭지점 그래서 블록크기 더해줌)
			if(tempB[i].x + Block.SIZE > rightWall) {
				rightCollision = true;
			}
		}
		
		//아래벽
		for(int i = 0; i < b.length; i++) {
			if(tempB[i].y + Block.SIZE > bottomFloor) {
				bottomCollision = true;
			}
		}
		
	}
	@Test
	public void checkStaticBlockCollision() { //쌓은블록확인용
		
		ArrayList<Block> staticBlocks = (battle ? PlayManager_Battle.staticBlocks : PlayManager.staticBlocks);
		
		
		for(int i = 0; i < staticBlocks.size(); i++) {
			//쌓인블록 하나씩 꺼내기
			int targetX = staticBlocks.get(i).x;
			int targetY = staticBlocks.get(i).y;
			
			//아래체크
			for(int ii = 0; ii < b.length; ii++) {
				if(b[ii].y + Block.SIZE == targetY && b[ii].x == targetX) { 
					//□ 지금 이상태임. 검은색은 이미 놓인거고, 하얀색은 내가 조종중인 블록.
					//■
					bottomCollision = true;
				}
			}
			
			//옆체크(왼쪽)
			for(int ii = 0; ii < b.length; ii++) {
				if(b[ii].x - Block.SIZE == targetX && b[ii].y == targetY) { 
					//■ □ 지금 이상태임. 검은색은 이미 놓인거고, 하얀색은 내가 조종중인 블록.
					leftCollision = true;
				}
			}
			
			//옆체크(오른쪽)
			for(int ii = 0; ii < b.length; ii++) {
				if(b[ii].x + Block.SIZE == targetX && b[ii].y == targetY) { 
					//□ ■ 지금 이상태임. 검은색은 이미 놓인거고, 하얀색은 내가 조종중인 블록.
					rightCollision = true;
				}
			}
			
			
		}
		
		
	}
	
	
	//battle 만져주기.
	@Test
	public void battleMino() {
		battle = true;
	}	
	
	
	//직접 움직이는 부분
	@Test
	public void handling() {
		
		boolean TURN = (battle ? KeyHandler_2.turnPressed_2 : KeyHandler.turnPressed);
		boolean DOWN = (battle ? KeyHandler_2.downPressed_2 : KeyHandler.downPressed);
		boolean LEFT = (battle ? KeyHandler_2.leftPressed_2 : KeyHandler.leftPressed);
		boolean RIGHT = (battle ? KeyHandler_2.rightPressed_2 : KeyHandler.rightPressed);
		
		//블록 움직이기
		TURN = true;
		if(TURN) {
			switch(1) {
			
			case 1 : getDirection2(); 
			case 2 : getDirection3(); 
			case 3 : getDirection4(); 
			case 4 : getDirection1();
			}
			
			battle = true;
			if (battle) {
				KeyHandler_2.turnPressed_2 = false;
			}
			
				KeyHandler.turnPressed = false;
			
			
		}
		
		checkMovementCollision();
		
		//밑에 내려도 되는 경우, 내림
		DOWN = true;
		if(DOWN) {
			bottomCollision = false;
			if (bottomCollision == false) {
				for(int i = 0 ; i < 4 ; i++) {
					b[i].y += Block.SIZE;
					
				}
				
				
				autoScoring();
			}
			
			//떨어지는 속도 조절시 오토드랍카운터는 초기화시켜줍시다.
			autoDropCounter = 0;
			battle = true;
			if (battle) {
				KeyHandler_2.downPressed_2 = false;
			}
			
				KeyHandler.downPressed = false;
			
		}
		
		
		LEFT = true;
		if(LEFT) {
			
			//왼쪽벽없을시, 이동
			if (leftCollision == false) {
				
				for(int i = 0 ; i < 4 ; i++) {
					b[i].x -= Block.SIZE;
				}
			}
			battle = true;
			if (battle) {
				KeyHandler_2.leftPressed_2 = false;
			}
			
				KeyHandler.leftPressed = false;
			
		}
		RIGHT = true;
		if(RIGHT) {
			
			//오른쪽벽없을시, 이동
			rightCollision = false;
			if (rightCollision == false) {
				
				for(int i = 0 ; i < 4 ; i++) {
					b[i].x += Block.SIZE;
				}
				
			}
			
			if (battle) {
				KeyHandler_2.rightPressed_2 = false;
			}
			
				KeyHandler.rightPressed = false;
			
		}
		
		
	}
	@Test
	public void autoScoring() {
		battle = true;
		if (battle) {
			PlayManager_Battle.score += PlayManager_Battle.level;
		}
		
			PlayManager.score += PlayManager.level;
		
	}

	//시간지나면 알아서 움직이는부분
	@Test
	public void handling_Auto() {
		bottomCollision = true;
		if(bottomCollision) {
			deactivating = true;
		}
	
			autoDropCounter++; // 매 프레임마다 증가
			if(autoDropCounter == PlayManager.dropInterval) {
				//60프레임이후 한칸 내려주기
				
				//일단 4블럭기준인데 여러블럭일경우 보정필요.
				for(int i = 0 ; i < 4 ; i++) {
					b[i].y += Block.SIZE;
				}
				
				autoDropCounter = 0; //초기화해줘서 0부터 반복.
			
		}
	}
	

	@Test
	public void update() {
		boolean SKILL = (battle ? KeyHandler_2.skillPressed_2 : KeyHandler.skillPressed);
		
		deactivating = true;
		if(deactivating) {
			deactivating();
		}
		
		
		//쾅 내리기
		SKILL = true;
		if(SKILL) {
			
			while (bottomCollision == false) {
				
				//일단 4블럭기준인데 여러블럭일경우 보정필요.
				for(int i = 0 ; i < 4 ; i++) {
					b[i].y += Block.SIZE;
				
				}
				
				
				checkMovementCollision(); 
			}
			
			//떨어지는 속도 조절시 오토드랍카운터는 초기화시켜줍시다.
			deactivateCounter = 1557;
			battle = true;
			if (battle) {
				KeyHandler_2.skillPressed_2 = false;
			}
			
				KeyHandler.skillPressed = false;
			
		}
		
		handling();
		
		//10줄라인 차는거 확인하는 용도
		
		/*
		if(bottomCollision) {
			deactivating = true;
		}
		*/
		handling_Auto();

	}
	
	
	
	//슬라이딩용 deactivating() 메소드
	@Test
	public void deactivating() {
		
		deactivateCounter ++;
		
		//슬라이딩은 10 frame 정도 시간을 줄게.
		deactivateCounter = 10 + 1;
		if(deactivateCounter >= 10) {
			
			deactivateCounter = 0;
			checkMovementCollision(); // 45프레임동안 대기후 바닥에 닿아있네? 바로 active 끄기.
			
			bottomCollision = true;
			if(bottomCollision) {
				active = false;
			}
		}
	}
	
	
//	public void draw(Graphics2D g2) {
//		
//		int margin = 2;
//		g2.setColor(b[0].c);
//		
//		
//		for(int i = 0 ; i < 4 ; i ++) {
//			
//			//itemL 보유중일경우 L 표시.
//			if(b[i].S) {
//				g2.setColor(Color.white);
//				g2.fillRect(b[i].x + margin, b[i].y + margin, Block.SIZE - (margin*2), Block.SIZE - (margin*2));
//
//				g2.setColor(b[i].c);
//
//				g2.drawRect(b[i].x + margin, b[i].y + margin, Block.SIZE - (margin*2), Block.SIZE - (margin*2));
//				
//				
//				g2.setColor(Color.black);
//				g2.drawString("S", b[i].x + margin, b[i].y + Block.SIZE - margin * 2);
//				
//				g2.setColor(b[i].c);
//				
//			}
//			
//			//itemL 보유중일경우 L 표시.
//			else if(b[i].L) {
//				g2.setColor(Color.white);
//				g2.fillRect(b[i].x + margin, b[i].y + margin, Block.SIZE - (margin*2), Block.SIZE - (margin*2));
//
//				g2.setColor(b[i].c);
//
//				g2.drawRect(b[i].x + margin, b[i].y + margin, Block.SIZE - (margin*2), Block.SIZE - (margin*2));
//				
//				
//				g2.setColor(Color.black);
//				g2.drawString("L", b[i].x + margin, b[i].y + Block.SIZE - margin * 2);
//				
//				g2.setColor(b[i].c);
//				
//			}
//			else {
//				
//				
//				g2.fillRect(b[i].x + margin, b[i].y + margin, Block.SIZE - (margin*2), Block.SIZE - (margin*2));
//			}
//		}
//		
//	}
	
}
