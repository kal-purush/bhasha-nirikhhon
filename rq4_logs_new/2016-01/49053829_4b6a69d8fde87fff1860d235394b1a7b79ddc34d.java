import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.util.ArrayList;
import java.util.EventObject;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ScrollPaneConstants;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.EventListenerList;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import javax.swing.text.TableView;

public class AndroidIdSetter {
	private ArrayList<ViewElement> list = null;
	private final static Class[] visiualElementType = {ViewContainer.class, ViewComponent.class, Menu.class};
	public AndroidIdSetter(ArrayList<ViewElement> list) {
		this.setList(list);
	}
	public ArrayList<ViewElement> getList() {
		return list;
	}
	public void setList(ArrayList<ViewElement> list) {
		this.list = list;
	}
	
	public void initSetter() {
		Toolkit kit=Toolkit.getDefaultToolkit();
		Dimension screensize=kit.getScreenSize();
		int screenheight=screensize.height;
		int screenwidth=screensize.width;

		JFrame frame = new JFrame();
		frame.setSize(screenwidth/2,screenheight/2);
		frame.setLocation(screenwidth/4,screenheight/4); 
		frame.setVisible(true);
		frame.setTitle("IdSetter");
		frame.setLayout(null);
		
		//panelΪ�����
		JPanel panel = new JPanel();
		panel.setLocation(40, 30);
		panel.setSize(screenwidth/2 - 100, screenheight/2 - 100);
		panel.setVisible(true);
		panel.setLayout(null);
		//panel.setBackground(Color.red);
		frame.add(panel);
		
		//����Ϊ�����ݰ󶨵�table��
		  
		SetterTableModel myModel = new SetterTableModel(list);
		JTable table = new JTable(myModel);
		table.setRowHeight(25);
		TableColumnModel tcm= table.getColumnModel();
        TableColumn tc = tcm.getColumn(3);
        //���á��Ա��еĵ�Ԫ����Ⱦ����renderer��
        tc.setCellRenderer(new AndroidTypeRenderer());
        tc.setCellEditor(new AndroidTypeEditor());
		//scorePanel������Ŵ���������table
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.setVisible(true);
		scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
		scrollPane.setSize(screenwidth/2 - 100, screenheight/2 - 150);
		panel.add(scrollPane);
		
		 

	}
	
	private class SetterTableModel  extends AbstractTableModel{
		/**
		 * 
		 */
		private static final long serialVersionUID = 7679117804117813045L;
		private String[] headName = { "ViewElementType", "Name", "ParentContainer", "AndroidType", "Id"};
		private Object[][] values = null;
		
		public SetterTableModel(ArrayList<ViewElement> list) {
			int columnLength = 0;
			if (null != list) {
				for (ViewElement e : list) {
					for (Class visiableClass : visiualElementType) {
						if (e.getClass().isAssignableFrom(visiableClass)) {
							columnLength ++;
							break;
						}
					}
				}
				values = new Object[columnLength][5];
				int index = 0;
				for (ViewElement e : list) {
					for (Class visiableClass : visiualElementType) {
						if (e.getClass().isAssignableFrom(visiableClass)) {
							//������ʱʹ����class������ΪViewElementType����������������������д���һ���ֶ�
							values[index][0] = e.getClass().toString().substring(6);
							values[index][1] = e.getName();
							ViewContainer directParent = e.getDirectParentContainer();
							values[index][2] = (null != directParent) ? directParent.getName() : "";
							values[index][3] = "";
							values[index][4] = e.getId();
							index++;
							break;
						}
					}
				}
			}
		}

		@Override
		public int getColumnCount() {
			// TODO Auto-generated method stub
			return values[0].length;
		}

		@Override
		public int getRowCount() {
			// TODO Auto-generated method stub
			return values.length;
		}

		@Override
		public Object getValueAt(int rowIndex, int columnIndex) {
			// TODO Auto-generated method stub
			return values[rowIndex][columnIndex];
		}
		
        public String getColumnName(int column){
            return headName[column];
        }
        
        public boolean isCellEditable(int row, int column){
        	if ((column == 0) || (column == 1) || (column ==2) ) {
        		return false;
        	}
            return true;
        }
        
        public void setValueAt(Object value, int row, int column){
            values[row][column]= value;
        }
	}
	
	public class AndroidTypeRenderer extends JComboBox implements TableCellRenderer{
        private static final long serialVersionUID = -8624401777277852691L;
        public AndroidTypeRenderer(){
                  super();
                  AndroidType[] list = AndroidType.values();
                  for (AndroidType type : list) {
                	  addItem(type);
                  }
        }
        public Component getTableCellRendererComponent(JTable table, Object value,
                           boolean isSelected, boolean hasFocus, int row, int column) {
                  if(isSelected){
                           setForeground(table.getForeground());
                           super.setBackground(table.getBackground());
                  }else{
                           setForeground(table.getForeground());
                           setBackground(table.getBackground());
                  }
                  if ((null == value) || value.equals("")) {
                  	//��ΪNULL
                  	setSelectedIndex(0);
                  } else if (value.getClass() == AndroidType.class) {
                  	for (AndroidType type : AndroidType.values()) {
                  		if (type == value) {
                  			setSelectedIndex(type.ordinal());
                  		}
                  	}
                  }
                  return this;
        }

	}
	
	public class AndroidTypeEditor extends JComboBox implements TableCellEditor{
	       
        private static final long serialVersionUID = 5860619160549087886L;
        private final AndroidType[] androidType = AndroidType.values();
        //EventListenerList:����EventListener �б����ࡣ
        private EventListenerList listenerList = new EventListenerList();
        //ChangeEvent����֪ͨ����Ȥ�Ĳ������¼�Դ�е�״̬�ѷ������ġ�
        private ChangeEvent changeEvent = new ChangeEvent(this);
        public AndroidTypeEditor(){
        		super();
                for (AndroidType type : androidType) {
              	  addItem(type);
                }
                  //������ֹ�༭�������԰�����Ԫ���JTable�յ���Ҳ���Դӱ༭������������������JComboBox�����
                  /*addActionListener(newActionListener(){
                           publicvoid actionPerformed(ActionEvent e) {
                                    System.out.println("ActionListener");
                                    //��ͬstopCellEditing�����ǵ���fireEditingStopped()����
                                    fireEditingStopped();
                           }
                         
                  });*/
        }
        public void addCellEditorListener(CellEditorListener l) {
                  listenerList.add(CellEditorListener.class,l);
        }
        public void removeCellEditorListener(CellEditorListener l) {
                  listenerList.remove(CellEditorListener.class,l);
        }
        private void fireEditingStopped(){
                  CellEditorListener listener;
                  Object[]listeners = listenerList.getListenerList();
                  for(int i = 0; i < listeners.length; i++){
                           if(listeners[i]== CellEditorListener.class){
                                    //֮������i+1������Ϊһ��ΪCellEditorListener.class��Class���󣩣�
                                    //���ŵ���һ��CellEditorListener��ʵ��
                                    listener= (CellEditorListener)listeners[i+1];
                                    //��changeEventȥ֪ͨ�༭���Ѿ������༭
                                    //��editingStopped�����У�JTable����getCellEditorValue()ȡ�ص�Ԫ���ֵ��
                                    //���Ұ����ֵ���ݸ�TableValues(TableModel)��setValueAt()
                                    listener.editingStopped(changeEvent);
                           }
                  }
        }
        public void cancelCellEditing() {        
        }
        /**
         * �༭����һ����Ԫ���ٵ����һ����Ԫ��ʱ�����á�-------------����������
         */
        public boolean stopCellEditing() {
                  //����ע�͵������fireEditingStopped();��Ȼ����GenderEditor�Ĺ��캯���а�
                  //addActionListener()��ע��ȥ������ʱ������ֹ�༭������JComboBox��ã���
                  
                  fireEditingStopped();//������ֹ�༭������JTable���
                  return true;
        }
        /**
         * Ϊһ����Ԫ���ʼ���༭ʱ��getTableCellEditorComponent������
         */
        public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
            if ((null == value) || value.equals("")) {
            	//��ΪNULL
            	setSelectedIndex(0);
            } else if (value.getClass() == AndroidType.class) {
            	for (AndroidType type : androidType) {
            		if (type == value) {
            			setSelectedIndex(type.ordinal());
            		}
            	}
            }
        	return this;
        }
        /**
         * ѯ�ʱ༭�����Ƿ����ʹ�� anEvent ��ʼ���б༭��
         */
        public boolean isCellEditable(EventObject anEvent) {
            return true;
        }
        /**
         * ���Ӧ��ѡ�����༭�ĵ�Ԫ���򷵻�true�����򷵻� false��
         */
        public boolean shouldSelectCell(EventObject anEvent) {
            return true;
        }

        /**
         * ����ֵ���ݸ�TableValue��TableModel���е�setValueAt()����
         */
        public Object getCellEditorValue() {
        	
            return androidType[getSelectedIndex()];
        }
	} 
	
	public static void main(String[] arg) {
//		AndroidIdSetter a = new AndroidIdSetter(null);
//		a.initSetter();
		if(ViewContainer.class.isAssignableFrom(Menu.class)) {
			System.out.println("OK");
		}
	}
}