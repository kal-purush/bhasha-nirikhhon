package O5_practice.exercise5_discount_system;

public class Customer {
    private String name;
    private boolean member = false;
    private String memberType;

    public Customer(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public boolean isMember() {
        return this.member;
    }

    public void setMember(boolean member) {
        this.member = member;
    }

    public String getMemberType() {
        return this.memberType;
    }

    public void setMemberType(String memberType) {
        this.memberType = memberType;
    }

    public String toString() {
        String memberText = this.member
                ? " is a " + this.memberType + " member."
                : " is not a member.";
        return this.name + memberText;
    }
}