package os;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingConstants;
import javax.swing.SwingWorker;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JComboBox;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

public class osproject {

	public static JFrame frame;
	public static int w=1100,h=700;
	public static void main(String[] args) {
		frame=new JFrame("This PC");
		Body body=new Body();
		frame.setSize(w,h);
		frame.setLocationRelativeTo(null);
		frame.setIconImage(Toolkit.getDefaultToolkit().getImage(osproject.class.getResource("pc.jpg")));
		frame.setVisible(true);
		frame.setResizable(false);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().add(body);
		
		//body.panel.setLayout(null);
		
	}
}
class Body extends JPanel implements ActionListener, KeyListener
{
	public int x=0,y=0,increment=0,incre=0,temp=0;
    public JButton sd,rs,search,cancel;
    public JButton power;
    public JPanel panel;
    public JTextField searchField;
    public JLabel loadingOrNot;
    public JComboBox comboBox;
    public JScrollPane scroll;
    public String[] str;
    public String dir;
    public boolean loading=false,cancelOrNot=false,msEnd=true,runEnd=false;
    Date startTime,endTime;
    Box v=Box.createVerticalBox();
    public int d=0,dirIndex=0,cnt=0;
    Duration duration;
    Instant startdur,enddur;
    Cursor cursor=Cursor.getPredefinedCursor(Cursor.HAND_CURSOR);
	public Body()
	{
	    addKeyListener(this);
	    setFocusable(true);
	    setBackground(Color.WHITE);
	    setLayout(null);
	    
	    init();
	}
	public void setDir()
	{
		int c=0,z=1;
		File[] drives;
		drives=File.listRoots();
		c=drives.length;
		str=new String[c+1];
		str[0]="";
		for(File drive:drives)
		{
			str[z]=drive+"\\";
			z++;
		}
		comboBox.setModel(new DefaultComboBoxModel(str));
	}
	public void clearAll()
	{
		incre=0;
		v.removeAll();
		v.repaint();
		v=Box.createVerticalBox();
		
	}
	
	public void init()
	{
		panel = new JPanel();
		//panel.setBounds(10, 50, 1060, 560);
		panel.setBackground(Color.WHITE);
		panel.setLayout(new BorderLayout());
		//add(panel);
		
		cancel = new JButton("Cancel");
		cancel.setBounds(927, 18, 75, 20);
		cancel.setBackground(Color.WHITE);
		cancel.setForeground(Color.BLACK);
		cancel.setFont(new Font("Tahoma",Font.BOLD,11));
		cancel.setVisible(false);
		cancel.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				search.setEnabled(true);
				temp=1;
				incre=1;
				dirIndex=str.length+5;
				loadingOrNot.setText("");
		    	cancel.setVisible(false);
		    	search.setEnabled(true);
		    	runEnd=false;
			}
		});
		add(cancel);
		
		
		scroll=new JScrollPane();
		scroll.setBounds(10, 50, 1060, 560);
		scroll.setViewportView(panel);
		scroll.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		add(scroll);
		
		loadingOrNot = new JLabel();
		loadingOrNot.setBounds(860, 19, 62, 20);
		add(loadingOrNot);
		
		searchField = new JTextField();
		searchField.setText("");
		searchField.setBounds(10, 19, 680, 20);
		add(searchField);
		searchField.setColumns(10);
		
		search = new JButton("Search");
		search.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String searchName=searchField.getText();
				if(searchName.length()>0) {
					clearAll();
					cancel.setVisible(true);
					search.setEnabled(false);
					loadingOrNot.setText("loading...");
					loadingOrNot.paintImmediately(loadingOrNot.getVisibleRect());
					loading=true;
					searchFiles(searchName);
					loading=false;
				}
			}
		});
		search.setBackground(new Color(255,255,255));
		search.setForeground(Color.DARK_GRAY);
		search.setFont(new Font("Tahoma", Font.BOLD, 11));
		search.setBounds(780, 18, 75, 20);
		add(search);
		
		comboBox = new JComboBox();
		setDir();
		comboBox.setFont(new Font("Tahoma",Font.PLAIN,13));
		comboBox.setBackground(Color.WHITE);
		comboBox.setBounds(700, 18, 70, 20);
		add(comboBox);
		
		power=new JButton();
        power.setBounds(10,612,35,35);
        power.setToolTipText("Power");
        power.setCursor(cursor);
        ImageIcon icon=new ImageIcon(getClass().getResource("/images/power.jpg"));
        power.setIcon(icon);
        power.setVisible(true);
        power.setHorizontalTextPosition(SwingConstants.CENTER);
        power.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if(x==0)
				{
					sd.setVisible(true);
					rs.setVisible(true);
					x++;
				}
				else
				{
					sd.setVisible(false);
					rs.setVisible(false);
					x=0;
				}
			}
		});
        add(power);
        
        sd=new JButton();
        sd.setBounds(50,612,70,35);
        sd.setCursor(cursor);
        sd.setToolTipText("Shut down");
        ImageIcon icon2=new ImageIcon(getClass().getResource("/images/shutdown.png"));
        sd.setIcon(icon2);
        sd.setHorizontalTextPosition(SwingConstants.CENTER);
        sd.setVisible(false);
        sd.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				try
				{
					Runtime rt=Runtime.getRuntime();
					rt.exec("shutdown -s -t 3");
				}catch(Exception ex){
					ex.printStackTrace();
				}
			}
		});
        add(sd);
        
        rs=new JButton();
        rs.setBounds(125,612,70,35);
        rs.setCursor(cursor);
        rs.setToolTipText("Restart");
        ImageIcon icon3=new ImageIcon(getClass().getResource("/images/restart.png"));
        rs.setIcon(icon3);
        rs.setHorizontalTextPosition(SwingConstants.CENTER);
        rs.setVisible(false);
        rs.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try
                {
                    Runtime rt=Runtime.getRuntime();
                    rt.exec("shutdown -r -t 3");
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            }
        });
        add(rs);
	}
	
	public void searchFiles(String name)
	{
		int len;
		cnt=0;
		increment=0;
		len=str.length;
		dir=comboBox.getSelectedItem().toString();
		SwingWorker<Void, Void>worker = new SwingWorker<Void, Void>()
		{
			protected Void doInBackground() throws Exception {
				if(dir=="") 
				{
					List<File> f=new ArrayList<File>();
					for(dirIndex=len-1;dirIndex>0;dirIndex--)
					{
						cnt+=Fileslist(str[dirIndex],name);
					}
				}
				else
				{
					cnt+=Fileslist(dir, name);
				}
				loadingOrNot.setText("");
		    	cancel.setVisible(false);
		    	search.setEnabled(true);
		    	if(cnt==0)
		    		ShowMessage(name);
				return null;
			}
		};
		worker.execute();
	}
	public int Fileslist(String directoryName,String name) {
	    Path path=Paths.get(directoryName);
	    incre=0;
	    try
	    {
	    	SimpleFileVisitor<Path> vtor=new SimpleFileVisitor<Path>()
	    	{
	    		public FileVisitResult visitFile(Path file, BasicFileAttributes basicAttribute)
	    		{
	    			System.out.println(file);
	    			if(increment==1500)
						loadingOrNot.setText("loading.");
					else if(increment==3000)
						loadingOrNot.setText("loading..");
					else if(increment==4500){
						loadingOrNot.setText("loading...");
						increment=0;
					}
					increment++;
					String fName=file.getFileName().toString();
	    			if(fName.toLowerCase().contains(name.toLowerCase()))
	    			{
	    				incre=1;
	    				System.out.print(file.getFileName()+"             "+file+"              ");
	    				File thisFile=new File(file.toString());
	    				DateFormat sdf=new SimpleDateFormat("dd MMMM yyyy hh:mm a");
	    				String date=sdf.format(thisFile.lastModified());
	    				long l=thisFile.length();
	    				System.out.println("Date: "+date+"               Size: "+l);
	    				
	    				printName_Path(file);
						printInfo(l,date);
	    				
	    			}
	    			if(temp==1)
	    			{
	    				temp=0;
	    				return FileVisitResult.TERMINATE;
	    			}
	    			return FileVisitResult.CONTINUE;
	    		}
	    		public FileVisitResult visitFileFailed(Path p, IOException exp)
	    		{
	    			if(temp==1)
	    			{
	    				temp=0;
	    				return FileVisitResult.TERMINATE;
	    			}
	    			return FileVisitResult.CONTINUE;
	    		}
	   		};
	   		EnumSet<FileVisitOption> opt=EnumSet.of(FOLLOW_LINKS);
	   		Files.walkFileTree(path, opt, Integer.MAX_VALUE, vtor);
	    }
	    catch(Exception exp) {}
	    return incre;
    }
	public void ShowMessage(String str)
	{
		JLabel label=new JLabel();
		label.setFont(new Font("Tahoma",Font.PLAIN,14));
		String extension="";
		try
		{
			if(str!=null)
			{
				extension=str.substring(str.lastIndexOf('.'));
			}
		}catch(Exception e) {
			extension="";
		}
		if(extension=="") {
			label.setText("No match found.");
			label.setHorizontalAlignment(SwingConstants.CENTER);
			JOptionPane.showMessageDialog(null, label, "File Search Result", JOptionPane.PLAIN_MESSAGE);
		}
		else{
			label.setText("No "+str+" file found.");
			label.setHorizontalAlignment(SwingConstants.LEFT);
			JOptionPane.showMessageDialog(null, label, "File Search Result", JOptionPane.ERROR_MESSAGE);
		}
	}
	public void printName_Path(Path file)
	{
		JPanel pan=new JPanel(new BorderLayout());
		pan.setBackground(Color.WHITE);
		pan.setLayout(new BoxLayout(pan, BoxLayout.X_AXIS));
		pan.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
		pan.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e)
			{
				long zzz=300;
				if(d==0){
					d++;
					startdur=Instant.now();
				}
				else if(d==1) {
					enddur=Instant.now();
					duration=Duration.between(startdur, enddur);
					zzz=duration.toMillis();
					if(zzz>=200)
						startdur=enddur;
					else
						d=0;
				}
				if(e.getButton()==e.BUTTON1 && zzz<200) {
					System.out.println(file.getFileName().toString());
					openClickedFile(file);
				}
			}
		});
		
		JLabel l1=new JLabel(file.getFileName().toString());
    	l1.setPreferredSize(new Dimension(320,27));
    	l1.setMaximumSize(new Dimension(320,27));
    	l1.setFont(new Font("Tahoma",Font.PLAIN,14));
    	pan.add(l1);
    	
    	JLabel l2=new JLabel("  "+file.toString());
    	l2.setPreferredSize(new Dimension(700,27));
    	l2.setMaximumSize(new Dimension(700,27));
    	l2.setFont(new Font("Tahoma",Font.PLAIN,14));
    	pan.add(l2);
    	
    	v.add(pan);
		panel.add(v,BorderLayout.PAGE_START);
	}
	public void printInfo(long len,String str2)
	{
		String l="";
		if(len%1000>=500) len=(len/1000)+1;
		else len=len/1000;
		l=Long.toString(len);
		l=l+" KB";
		if(len>99999) { 
			len=len/1000;
			l=Long.toString(len);
			l=l+" MB";
		}
		
		JPanel pan=new JPanel(new BorderLayout());
    	pan.setBackground(Color.WHITE);
    	pan.setLayout(new BoxLayout(pan, BoxLayout.X_AXIS));
    	
    	JLabel l1=new JLabel("Size: "+l);
    	l1.setPreferredSize(new Dimension(320,15));
    	l1.setMaximumSize(new Dimension(320,15));
    	l1.setFont(new Font("Tahoma",Font.PLAIN,12));
    	pan.add(l1);
    	
    	JLabel l2=new JLabel("  Last Modified: "+str2);
    	l2.setPreferredSize(new Dimension(700,15));
    	l2.setMaximumSize(new Dimension(700,15));
    	l2.setFont(new Font("Tahoma",Font.PLAIN,12));
    	pan.add(l2);
    	
    	pan.setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, Color.LIGHT_GRAY));
    	
    	v.add(pan);
		panel.add(v,BorderLayout.PAGE_START);
		validate();
		panel.validate();
	}
	public void openClickedFile(Path file)
	{
		try {
			File openFile=new File(file.toString());
			if(!Desktop.isDesktopSupported())
			{
				JLabel label=new JLabel("Not Supported");
				label.setFont(new Font("Tahoma",Font.BOLD,14));
				label.setHorizontalAlignment(SwingConstants.LEFT);
				JOptionPane.showMessageDialog(null, label, "Opening File", JOptionPane.ERROR_MESSAGE);
				return;
			}
			Desktop dt=Desktop.getDesktop();
			if(openFile.exists()){
				dt.open(openFile);
			}
		}
		catch(Exception exc) {exc.printStackTrace();}
	}

	@Override
	public void keyTyped(KeyEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void keyPressed(KeyEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void keyReleased(KeyEvent e) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		// TODO Auto-generated method stub
		
	}
}