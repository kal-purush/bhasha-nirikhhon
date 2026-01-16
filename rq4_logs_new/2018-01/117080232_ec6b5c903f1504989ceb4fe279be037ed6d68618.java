package nrega.worker.Model;

/**
 * Created by HP on 22-01-2018.
 */

public class FamilyMember {
    private String name;
    private String DOB;
    private String bankName;

    public FamilyMember(String name, String DOB, String bankName) {
        this.name = name;
        this.DOB = DOB;
        this.bankName = bankName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDOB() {
        return DOB;
    }

    public void setDOB(String DOB) {
        this.DOB = DOB;
    }

    public String getBankName() {
        return bankName;
    }

    public void setBankName(String bankName) {
        this.bankName = bankName;
    }
}