package ATM_project;
import java.util.InputMismatchException;
import java.util.*;
class Account
{

    private int CN;
    private int PN;
    void setCustomerNumber(int cn)
    {
        CN=cn;
    }
    void setPINNumber(int PN)
    {
        this.PN=PN;
    }
    int getCustomerNumber()
    {
        return CN;
    }
    int getPINNumber()
    {
        return PN;
    }

}
class OptionMenu extends Account
{
    Scanner op1=new Scanner(System.in);
    HashMap<Integer,Integer> op3=new HashMap();
    void getLogin()
    {
        try
        {
            op3.put(11111, 111);
            op3.put(11112, 222);
            op3.put(11113, 333);
            op3.put(11114, 444);
            op3.put(11115, 111);

            System.out.println("Welcome To The Sinhgad ATM");
            System.out.print("\nEnter your Customer Number : ");
            setCustomerNumber(op1.nextInt());
            System.out.print("Enter your PIN Number : ");
            setPINNumber(op1.nextInt());
        }
        catch(InputMismatchException op2)
        {
            System.out.println("\nPlease enter only numbers.\ncharacters and symbols are not allowed.");
        }
        int A=getCustomerNumber();
        int B=getPINNumber();
        if(op3.containsKey(A)&&op3.get(A)==B)
        {
            System.out.println("Aaj ke liye itna hi");
        }
        else
        {
            System.out.println("Unsuccessful");
        }
    }
}
public class ATM extends OptionMenu
{
    public static void main(String[] args)
    {
        ATM op=new ATM();
        op.getLogin();
    }
}