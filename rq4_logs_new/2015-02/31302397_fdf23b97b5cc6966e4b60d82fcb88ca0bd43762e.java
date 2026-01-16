package Xpace;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
public class XDevice extends Xpace{
    public static enum Operation{ON,OFF,SLEEP,SHUTDOWN};
    public String hostname;
    public Operation operation;    
    public List<XPort>ports=new LinkedList<XPort>();
    public  int x;
    public int y;
    public int width;
    public int height;
    
    
    public XDevice(){
        this.hostname="XDevice";
        System.out.println("Build XDevice");
    } 
    
    public boolean addXPorts(XPort port){
        if(port !=null){
            ports.add(port);
        return true;
        }else{
            return false;
            
        }
    }
    public boolean showXPorts(){
     
        Iterator<XPort> portNavi=ports.iterator();
        while(portNavi.hasNext()){
            
            XPort p=portNavi.next();
          
            System.out.println(p);
            
        }
        
        return true;
    }
    @Override
    public String toString(){
        return this.hostname;
    }
    
}