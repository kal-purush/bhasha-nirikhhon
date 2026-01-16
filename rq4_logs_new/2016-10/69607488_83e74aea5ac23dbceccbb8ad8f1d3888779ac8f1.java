/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Program;

/**
 *
 * @author hment
 */
public class GetMessage {
    private final Student s1;
    private final Professor p1;
    private final BusinessPerson bp1;
    private final Blogger b1;
    
    public GetMessage(Student s, Professor p, BusinessPerson bp, Blogger b){
        s1=s;
        p1=p;
        bp1=bp;
        b1=b;
    }
    
    public  String getMessage(String str1,String str2){
        String result;
         switch(str1){
                case "Student":
                    result= s1.preference(str2);
                    break;
                case "Professor":
                    result = p1.preference(str2);
                    break;
                case "BusinessPerson":
                    result = bp1.preference(str2);
                    break;
                case "Blogger":
                    result=b1.preference(str2);
                    break;
                default:
                    result="error in getMessage method!";
            }
            return result;
        }
}
  