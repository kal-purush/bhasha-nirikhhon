public class Lend_Borrow {

    private String personName;
    private String type;/*whether a person has borrowed or lent money*/
    private float amount;
    private String tag;//applicable for loans only

    public String returnpersonName() {
        return personName;
    }
    public String returnType() {
        return type;
    }
    public float returnAmount() {
        return amount;
    }
    public String returnLoanNumber() {
        return tag;
    }

    /*Constructor for loan*/
    Lend_Borrow (String type,String personName,float amount,String tag) {
        this.type=type;
        this.personName=personName;
        this.amount=amount;
        this.tag=tag;
    }
}