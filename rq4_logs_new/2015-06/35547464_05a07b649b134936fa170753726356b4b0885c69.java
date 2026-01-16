package presentation.compare;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Font;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Vector;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import presentation.component.BgPanel;
import presentation.component.ChartCheckBox;
import presentation.component.GLabel;
import presentation.contenui.UIUtil;
import bussinesslogic.Regression;
import bussinesslogic.player.PlayerLogic_db;
import data.po.PersonR;
import data.po.RegreResult;
import data.po.playerData.PlayerDetailInfo;

public class PlayerAnalysis extends BgPanel{

	private static final long serialVersionUID = 1L;
	private GLabel player;
	private PlayerLogic_db playerLogic_db = new PlayerLogic_db();;
	private PlayerDetailInfo playerDetailInfos;
	private int id;
	private boolean[] select;
	private boolean isFirst;
	private GLabel selectLabel, VS, dataLebel, search, bg, warn;
	private ChartCheckBox[] chartChekBox;
	private JComboBox<String> search0;
	private String[] dataType = {"在场时间", "命中％", "三分进球数", "罚篮进球数", "前场篮板"};
	private String[] dataTypeFunction = {"time","shootper","th_in","ft_in","offb"};
	private String[] dataResult = {"随即误差","平均标准偏差","相关系数r","回归偏差平方和","总偏差平方和","F值"};
	private ChartPanel panel;
	private Vector<String> playerNames = new Vector<>();
	private Regression regression = new Regression();
	private RegreResult regreResult;

	public static void main(String[] args) {
		SwingUtilities.invokeLater(new Runnable()
		{
			public void run()
			{
				JFrame frame = new JFrame();  
				frame.setSize(1000,700);
				frame.setVisible(true); 
				frame.setLayout(null);

				JPanel panel = new JPanel();
				panel.setBounds(0, 0, 1000, 700);
				panel.setBackground(UIUtil.lightGrey);
				panel.setLayout(null);

				PlayerAnalysis panel2 = new PlayerAnalysis();

				panel.add(panel2);
				frame.add(panel);
			}
		});
	}
	
	public PlayerAnalysis(){
		super("");

		this.setBounds(0, 0, 940, 600);
		this.setBackground(Color.white);
		this.setLayout(null);
		this.setVisible(true);

		isFirst = true;

		init();
	}

	private void init(){
		ArrayList<PlayerDetailInfo> list = new ArrayList<>();
		try {
			list = playerLogic_db.getAlldetail("null");
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		playerNames.add("球员英文名模糊查询");
		for(int i=0;i<list.size();i++){
			playerNames.add(list.get(i).getName());
		}

		search0 = new JComboBox<>(playerNames);
		search0.setEditable(true);
		search0.setBounds(110, 110, 200, 25);
		search0.getEditor().getEditorComponent().addKeyListener(new KeyListener() {
			@Override
			public void keyTyped(KeyEvent e) {
			}
			@Override
			public void keyReleased(KeyEvent e) {
				if((e.getKeyChar()>='a'&&e.getKeyChar()<='z')||(e.getKeyChar()>='A'&&e.getKeyChar()<='Z')||e.getKeyChar()==8){
					JTextField textField = (JTextField) search0.getEditor().getEditorComponent();
					String str = textField.getText();
					search0.setModel(new DefaultComboBoxModel<>(searchJundge(str)));
					search0.setPopupVisible(true);
				}
			}
			@Override
			public void keyPressed(KeyEvent e) {
			}
		});
		search0.getEditor().getEditorComponent().addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e){
				JTextField textField = (JTextField) search0.getEditor().getEditorComponent();
				String str = textField.getText();
				if(str.equals("球员英文名模糊查询")){
					playerNames.set(0, "");
					search0.setModel(new DefaultComboBoxModel<>(playerNames));
				}
			}
		});
		this.add(search0);

		search = new GLabel("分析", new Point(110, 165), new Point(200, 25), this, true, 1, 15);
		search.setForeground(UIUtil.nbaBlue);
		search.setCursor(new Cursor(Cursor.HAND_CURSOR));
		search.setHorizontalAlignment(JLabel.CENTER);
		search.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e){
				String name0 = search0.getEditor().getItem().toString();

				try {
					id = playerLogic_db.getIDbyName(name0, "");

					if((id == 0)){
						removeData();
						removeChart();
						bg.setVisible(false);
						warn.setVisible(true);
						selectLabel.setVisible(false);
						dataLebel.setVisible(false);
						isFirst = true;
					}else{
						try {
							regreResult = regression.sqt(id);
						} catch (RemoteException e2) {
							e2.printStackTrace();
						}
						if(regreResult.getV().length == 0){
							removeData();
							removeChart();
							bg.setVisible(false);
							warn.setVisible(true);
							selectLabel.setVisible(false);
							dataLebel.setVisible(false);
							isFirst = true;
						}else{
							bg.setVisible(false);
							warn.setVisible(false);
							selectLabel.setVisible(true);
							dataLebel.setVisible(true);
							chartInit();
							dataInit();
							isFirst = false;
						}
					}
				} catch (RemoteException e1) {
					e1.printStackTrace();
				}

			}
		});

		dataLebel = new GLabel("", new Point(520, 260), new Point(420, 340), this, false);

		bg = new GLabel("img/analysis/bg.png", new Point(240, 100), new Point(500, 500), this, true);
		warn = new GLabel("img/analysis/warn.png", new Point(220, 100), new Point(500, 500), this, false);

		VS = new GLabel("VS", new Point(420, 60), new Point(100, 200), this, false, 1, 35);
		VS.setHorizontalAlignment(JLabel.CENTER);
		VS.setForeground(UIUtil.nbaBlue);

		selectLabel = new GLabel("", new Point(420, 260), new Point(100, 340), this, false);

		chartChekBox = new ChartCheckBox[dataType.length];
		select = new boolean[dataType.length];
		for(int i=0;i<dataType.length;i++){
			select[i] = false;
			chartChekBox[i] = new ChartCheckBox(i);

			chartChekBox[i].setBounds(0, 50+25*i, 100, 25);

			chartChekBox[i].setText(dataType[i]);
			chartChekBox[i].setEnabled(false);
			chartChekBox[i].setBackground(UIUtil.bgWhite);
			chartChekBox[i].addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					isFirst = false;
					ChartCheckBox chartChekBox = (ChartCheckBox)e.getSource();
					if(!chartChekBox.isSelected()){
						chartChekBox.setSelected(true);
					}else{
						setFalse();
						select[chartChekBox.getNumber()] = true;
						chartChekBox.setSelected(true);
					}
					chartInit();
				}
			});
			selectLabel.add(chartChekBox[i]);
		}
	}

	private void setFalse(){
		for(int i=0;i<dataType.length;i++){
			select[i] = false;
			chartChekBox[i].setSelected(false);
		}
	}

	private Vector<String> searchJundge(String str){
		Vector<String> names = new Vector<>();
		names.add(str);

		for(int i=1;i<playerNames.size();i++){
			if(playerNames.get(i).toLowerCase().contains(str.toLowerCase())){
				names.add(playerNames.get(i));
			}
		}

		return names;
	}

	private void removeData(){
		if(!isFirst){
			dataLebel.removeAll();
			player.removeAll();
		}

		this.updateUI();
	}
	
	private void removeChart(){
		if(!isFirst){
			this.remove(panel);
		}

		this.updateUI();
	}

	private void dataInit(){
		removeData();
		
		try {
			playerDetailInfos = playerLogic_db.getdetail(id);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		player = new GLabel("", new Point(520, 60), new Point(420, 200), this, true);

		GLabel pic = new GLabel("img/players/"+id+".jpg", new Point(20, 10), new Point(180, 180), player, true);
		GLabel name = new GLabel(playerDetailInfos.getName(), new Point(220, 10), new Point(200, 30), player, true, 0, 15);
		GLabel birth = new GLabel(playerDetailInfos.getBirth(), new Point(name.getLocation().x, name.getLocation().y+name.getSize().height+5), new Point(name.getSize().width, name.getSize().height), player, true, 0, 15);
		GLabel height = new GLabel(playerDetailInfos.getHeight(), new Point(birth.getLocation().x, birth.getLocation().y+birth.getSize().height+5), new Point(name.getSize().width, name.getSize().height), player, true, 0, 15);
		GLabel weight = new GLabel(playerDetailInfos.getWeight(), new Point(height.getLocation().x, height.getLocation().y+height.getSize().height+5), new Point(name.getSize().width, name.getSize().height), player, true, 0, 15);
		GLabel position = new GLabel(playerDetailInfos.getPosition(), new Point(weight.getLocation().x, weight.getLocation().y+weight.getSize().height+5), new Point(name.getSize().width, name.getSize().height), player, true, 0, 15);

		GLabel fx = new GLabel("多元线性回归方程:", new Point(30, 25), new Point(400, 30), dataLebel, true, 0, 15);
		JTextArea fxi = new JTextArea(regreResult.getEquation());
		fxi.setBounds(30, 60, 400, 60);
		fxi.setBackground(UIUtil.bgWhite);
		fxi.setEditable(false);
		fxi.setLineWrap(true);
		fxi.setWrapStyleWord(true);
		fxi.setFont(new Font("微软雅黑", 0, 15));
		fxi.setBorder(null);
		fxi.setOpaque(false);
		dataLebel.add(fxi);
		
		GLabel xr = new GLabel("偏相关系数", new Point(140, 100), new Point(90, 30), dataLebel, true, 0, 15);
		xr.setHorizontalAlignment(JLabel.CENTER);
		for(int i=0;i<regreResult.getValue().size();i++){
			GLabel x = new GLabel("X"+i+": "+change(regreResult.getValue().get(i).getType()), new Point(30, 130+i*35), new Point(120, 30), dataLebel, true, 0, 15);
			GLabel xir = new GLabel(String.valueOf(regreResult.getV()[i]), new Point(140, 130+i*35), new Point(90, 30), dataLebel, true, 0, 15);
			xir.setHorizontalAlignment(JLabel.CENTER);
		}
		
		Point size = new Point(400, 30);
		GLabel q = new GLabel(dataResult[0]+": "+regreResult.getDt()[0], new Point(240, 130), size, dataLebel, true, 0, 15);
		GLabel s = new GLabel(dataResult[1]+": "+regreResult.getDt()[1], new Point(q.getLocation().x, q.getLocation().y+size.y+5), size, dataLebel, true, 0, 15);
		GLabel r = new GLabel(dataResult[2]+": "+regreResult.getDt()[2], new Point(s.getLocation().x, s.getLocation().y+size.y+5), size, dataLebel, true, 0, 15);
		GLabel u = new GLabel(dataResult[3]+": "+regreResult.getDt()[3], new Point(r.getLocation().x, r.getLocation().y+size.y+5), size, dataLebel, true, 0, 15);
		GLabel tu = new GLabel(dataResult[4]+": "+regreResult.getDt()[4], new Point(u.getLocation().x, u.getLocation().y+size.y+5), size, dataLebel, true, 0, 15);
		GLabel f = new GLabel(dataResult[5]+": "+regreResult.getF(), new Point(tu.getLocation().x, tu.getLocation().y+size.y+5), size, dataLebel, true, 0, 15);
	}
	
	private void chartInit(){
		removeChart();

		ArrayList<PersonR> list = regreResult.getValue();System.out.println(list.size());

		if(isFirst){
			for(int i=0;i<list.size();i++){
				for(int j=0;j<dataType.length;j++){
					if(list.get(i).getType().equals(dataTypeFunction[j])){
						chartChekBox[j].setEnabled(true);
					}
				}
			}
			for(int i=0;i<chartChekBox.length;i++){
				if(chartChekBox[i].isEnabled()){
					chartChekBox[i].setSelected(true);
					break;
				}
			}
		}

		int k = 0, p=0;
		for(int i=0;i<chartChekBox.length;i++){
			if(!chartChekBox[i].isEnabled()){
				p++;
			}
			if(chartChekBox[i].isSelected()){
				k=i-p;
				break;
			}
		}

		XYSeriesCollection localXYSeriesCollection = new XYSeriesCollection();
		XYSeries localXYSeries1 = new XYSeries(change(regreResult.getValue().get(k).getType()));

		for(int j=0;j<regreResult.getY().length;j++){
			localXYSeries1.add(regreResult.getX()[k][j],regreResult.getY()[j]);
		}
		localXYSeriesCollection.addSeries(localXYSeries1);

		MyDemoPanel2 MyDemoPanel2= new MyDemoPanel2(localXYSeriesCollection);
		panel = MyDemoPanel2.getPanel();
		panel.setBounds(0, 260, 420, 335);
		this.add(panel);
		
		this.updateUI();
	}
	
	private String change(String str){
		switch (str) {
		case "time":return "在场时间";
		case "shootper":return "命中％";
		case "th_in":return "三分进球数";
		case "ft_in":return "罚篮进球数";
		case "offb":return "前场篮板";
		default:return "";
		}
	}

}

class MyDemoPanel2{
	private ChartPanel panel;

	public MyDemoPanel2(XYDataset xyDataset){
		super();
		this.panel = createChartPanel(xyDataset);
	}

	private ChartPanel createChartPanel(XYDataset xyDataset){
		JFreeChart localJFreeChart = ChartFactory.createScatterPlot("points analysis", "X", "points", xyDataset, PlotOrientation.VERTICAL, true, true, false);
		XYPlot localXYPlot = (XYPlot)localJFreeChart.getPlot();
		localXYPlot.setDomainCrosshairVisible(true);
		localXYPlot.setDomainCrosshairLockedOnData(true);
		localXYPlot.setRangeCrosshairVisible(true);
		localXYPlot.setRangeCrosshairLockedOnData(true);
		localXYPlot.setDomainZeroBaselineVisible(true);
		localXYPlot.setRangeZeroBaselineVisible(true);
		localXYPlot.setDomainPannable(true);
		localXYPlot.setRangePannable(true);
		NumberAxis localNumberAxis = (NumberAxis)localXYPlot.getDomainAxis();
		localNumberAxis.setAutoRangeIncludesZero(false);
		ChartUtilities.applyCurrentTheme(localJFreeChart);
		ChartPanel localChartPanel = new ChartPanel(localJFreeChart);
		return localChartPanel;
	}

	public ChartPanel getPanel(){
		return this.panel;
	}
}
