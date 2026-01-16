
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;
import com.cn.Chess;
import com.cn.ChessPlan;
import com.cn.ChessChange;
public class ChessFive1 extends Panel implements MouseListener//,ActionListener
{
	ServerSocket server=null;
	Socket so=null;
	DataInputStream dataIn=null;
	DataOutputStream dataout=null;
	Panel p,p2;
	Label l1,l2,l3,l4;
	TextArea t1;
	TextField t2,t3,t4;
	boolean s = true;
	int[][] chess = new int[15][15];
	int bWin=1;
	Button bStart;
	Button bServerStart;
	Button bSendMessage;
	Chess ch;
	ChessFive1() throws Exception
	{
		p = new Panel();
		p2 = new Panel();
		//bServer = new bServer();
		p2.setBounds(500, 30, 200, 50);
		p.setBounds(30, 465, 720, 80);
		p.setBackground(Color.WHITE);
		bStart = new Button("��ʼ");
		bServerStart = new Button("����");
		bSendMessage = new Button("������Ϣ");
		l1 = new Label("�˿�:");
		l2 = new Label("��Ϣ:");
		l3 = new Label("�س�:");
		l4 = new Label("���ݴ�ѧ ��������ѧԺ ��������ϵ 2009.6.5 http://www.sise.com.cn");
		t2 = new TextField("5000",6);
		t3 = new TextField("��� hello",32);
		t4 = new TextField("˭˭˭",12);
		p2.add(bStart);
		add(p2);
		p.setLayout(new FlowLayout());
		p.add(l1);
		p.add(t2);
		p.add(l2);
		p.add(t3);
		p.add(bSendMessage);		
		p.add(bServerStart);
		p.add(l3);
		p.add(t4);
		p.add(l4);

		add(p);
		
		t1 = new TextArea();
		
		setSize(780, 500);
		setBackground(Color.GREEN);
		addMouseListener(this);
		setLayout(null);
		t1.setBounds(470, 100, 280, 350);
		add(t1);
		t1.append("��Һ�! \n");
		bStart.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent Event)
			{
				for(int i=0;i<15;i++)
				{
				for(int j=0;j<15;j++)
				{
					chess[i][j]=0;
				}
		}			
		repaint();
		t1.setText("");
		//READY=false;
		bWin=1;
			}
		}
		);
		class accept extends Thread
                {
                    public void run()
			{	
				//Graphics2D g=new Graphics2D();
				while(true)
				{
						try
						{
							/*t1.append(dataIn.readUTF()+"\n");
							repaint();*/
							String[] ss=dataIn.readUTF().toString().split(":");
							conn(ss);
			
						}
						catch(IOException gg){} 			
				}	
			}
		  public void conn(String[] s)
		  {
			if(s[0].equals("chat"))
			{
			for(int i=1;i<s.length;i++)
			{
					t1.append(s[i]+" ");
			}
			t1.append("\r\n");
			}
			else if (s[0].equals("OK"))
			{
				try{dataout.flush();}catch(Exception e1){};
			}
			else if (s[0].equals("win"))
			{
				t1.append("��ϲ,�췽ʤ!!\n");
				bWin=0;
			}
			else if(s[0].equals("chess"))
			{
			ChessChange.ibool=true;
			ChessChange.x=Integer.parseInt(s[1]);
			ChessChange.y=Integer.parseInt(s[2]);
			chess[ChessChange.x-1][ChessChange.y-1]=Integer.parseInt(s[3]);
			repaint();	
			t1.append("�Է����壺"+"("+s[1]+","+s[2]+")"+"\r\n");
			win(ChessChange.x,ChessChange.y);
			try{dataout.writeUTF("OK" + ":" + ")\n");}catch(Exception e1){}
			}
		  }

                }
		bServerStart.addActionListener(new ActionListener() 
				{
					public void actionPerformed(ActionEvent e) 
					{
						try
						{
							server=new ServerSocket(5000);
							so=server.accept();	
							dataIn=new DataInputStream(so.getInputStream());
							dataout=new DataOutputStream(so.getOutputStream());
							accept tt = new accept();
							tt.start();
						}
						catch(Exception e1){}
					}
				}
           			);
		bSendMessage.addActionListener(new ActionListener()
				{
					public void actionPerformed(ActionEvent e)
					{
						try
			  		 	{
						   	dataout.writeUTF("chat:"+ ":" + t4.getText() + ":" + t3.getText());
			  	 		 	t1.append(t4.getText()+t3.getText()+"\n");
			  			}
			  			catch(IOException e1) {}    	  
					}
				}
				);
		
		
	}

	public void paint(Graphics g)
	{
		for (int i = 30; i <= 450; i = i + 30)
		{
			g.drawLine(30, i, 450, i);
			g.drawLine(i, 30, i, 450);
		}

		for (int k = 30; k <= 450; k += 30)
		{
			g.drawString(String.valueOf(k / 30), k - 5, 20);
			g.drawString(String.valueOf(k / 30), 10, k + 5);
		}

		for (int i = 0; i < 15; i++)
		{
			for (int j = 0; j < 15; j++)
			{
				if (chess[i][j] == 1)
				{
					//ch=new Chess(i,j);
					//ChessPlan.chessplan.add(ch);
					g.setColor(Color.red);
					g.fillOval((i + 1) * 30 - 12, (j + 1) * 30 - 12, 24, 24);
				}
				else if (chess[i][j] == 10)
				{
					//ch=new Chess(i,j);
					//ChessPlan.chessplan.add(ch);
					g.setColor(Color.black);
					g.fillOval((i + 1) * 30 - 12, (j + 1) * 30 - 12, 24, 24);
				}
			   
			}
		}
		/*for(Chess c:ChessPlan.chessplan){
				if (chess[c.x][c.y] == 1)
				{
					g.setColor(Color.red);
					g.fillOval((c.x + 1) * 30 - 12, (c.y + 1) * 30 - 12, 24, 24);
				}
				else if (chess[c.x][c.y] == 10)
				{
					g.setColor(Color.black);
					g.fillOval((c.x + 1) * 30 - 12, (c.y + 1) * 30 - 12, 24, 24);
				}
			   }*/
	}
	public void mousePressed(MouseEvent e)
	{
		int x = (int)e.getX();
		int y = (int)e.getY();
		int a = (x + 15) / 30;
		int b = (y + 15) / 30;
		if (a > 0 && a <= 15 && b > 0 && b <= 15 && chess[a - 1][b - 1] == 0)
		{
			if (bWin==1&&ChessChange.ibool)
			{
				chess[a - 1][b - 1] = 10;
				try{dataout.writeUTF("chess" + ":" +a +":" + b +":"+ "10" + ":"  + ")\n");}catch(Exception e1){}
				t1.append("�ڷ�����λ��:(" + a + "," + b + ")\n");
				ChessChange.ibool=false;
			}
			if (win(a - 1, b - 1) == 2)
			{
				System.out.println("��ϲ,�ڷ�ʤ!!");
				t1.append("��ϲ,�ڷ�ʤ!\n");
				bWin=0;
				try{dataout.writeUTF("win" + ":" +  ")\n");}catch(Exception e1){}
			}	
			if (ChessChange.ibool)
			{
				System.out.println("1");
			}
			else System.out.println("0");
		}
		repaint();
	}

	public void mouseReleased(MouseEvent e) { }
	public void mouseEntered(MouseEvent e) { }
	public void mouseExited(MouseEvent e) { }
	public void mouseClicked(MouseEvent e) { }

	//�������ж���Ӯ����
	int win(int x, int y)
	{
		final int[][] cc ={ { 0, 1 }, { 1, 1 }, { 1, 0 }, { 1, -1 } };
		final int[][] dd ={ { 0, -1 }, { -1, -1 }, { -1, 0 }, { -1, 1 } };

		int s, s1, s2, x1, y1;
		if (x >= 0 && x < 15 && y >= 0 && y < 15)
		{
			for (int i = 0; i < 4; i++)
			{
				s = chess[x][y];
				s1 = chess[x][y];
				s2 = chess[x][y];
				x1 = x;
				y1 = y;
				x1 += cc[i][0];
				y1 += cc[i][1];
				if ((x1 >= 0) && (x1 < 15) && (y1 >= 0) && (y1 < 15))
				{
					while (chess[x1][y1] == s)
					{
						s1 += chess[x1][y1];
						x1 += cc[i][0];
						y1 += cc[i][1];
						if (x1 < 0 || x1 > 14 || y1 < 0 || y1 > 14) break;
					}
				}
				x1 = x;
				y1 = y;

				x1 += dd[i][0];
				y1 += dd[i][1];

				if ((x1 > 0) && (x1 < 15) && (y1 > 0) && (y1 < 15))
				{
					while (chess[x1][y1] == s)
					{
						s2 += chess[x1][y1];
						x1 += dd[i][0];
						y1 += dd[i][1];
						if (x1 < 0 || x1 > 14 || y1 < 0 || y1 > 14) break;
					}
				}
				if ((s1 + s2) >= 6 && (s1 + s2) <= 10) return 1;
				if ((s1 + s2) >= 60) return 2;
			}
			return 0;
		}
		else
		{
			return 0;
		}
	}

}


class Test1 extends Frame
{
	ChessFive1 p;
	Test1(String s)throws Exception
	{
		super(s);
		p = new ChessFive1();
		setLayout(new BorderLayout());
		addWindowListener(new WindowAdapter()
		{
			public void windowClosing(WindowEvent e)
			{
				System.exit(0);
			}
		});
		setBounds(100, 100, 785, 600);
		add(p);
		setBackground(Color.WHITE);
		validate();
		setVisible(true);
	}
	public static void main(String[] args)throws Exception
	{
		new Test1("������");
	}
}