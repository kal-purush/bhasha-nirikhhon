package com.tia102g3.member.model;

import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.Objects;

public class Member implements Serializable {
    private Integer memberID;
    private byte[] personalPhotos;
    private String name;
    private String account;
    private String password;
    private String email;
    private String gender;
    private String phone;
    private String address;
    private Date bD;
    private Date regDate;
    private Integer cMemberID;
    private String receiver;
    private String receiverAddress;
    private String receiverPhone;
    private String cardName;
    private String cardValidTime;
    private String cardLast3No;
    private String cardPhone;

    public Member() {
    }

    public Member(Integer memberID, byte[] personalPhotos, String name, String account, String password, String email, String gender,
                  String phone, String address, Date bD, Date regDate, Integer cMemberID, String receiver, String receiverAddress,
                  String receiverPhone, String cardName, String cardValidTime, String cardLast3No, String cardPhone
    ) {
        this.memberID = memberID;
        this.personalPhotos = personalPhotos;
        this.name = name;
        this.account = account;
        this.password = password;
        this.email = email;
        this.gender = gender;
        this.phone = phone;
        this.address = address;
        this.bD = bD;
        this.regDate = regDate;
        this.cMemberID = cMemberID;
        this.receiver = receiver;
        this.receiverAddress = receiverAddress;
        this.receiverPhone = receiverPhone;
        this.cardName = cardName;
        this.cardValidTime = cardValidTime;
        this.cardLast3No = cardLast3No;
        this.cardPhone = cardPhone;
    }

    public Integer getMemberID() {
        return memberID;
    }

    public void setMemberID(Integer memberID) {
        this.memberID = memberID;
    }

    public byte[] getPersonalPhotos() {
        return personalPhotos;
    }

    public void setPersonalPhotos(byte[] personalPhotos) {
        this.personalPhotos = personalPhotos;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getEmail() {
        return email;
    }

    // Email regex pattern
//    private static final String EMAIL_PATTERN = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$";
//    private static final Pattern pattern = Pattern.compile(EMAIL_PATTERN);
//
//    public void setEmail(String email) throws Exception {
//        if (email == null || !pattern.matcher(email).matches()) {
//            throw new Exception("Invalid email format");
//        }
//        this.email = email;
//    }
    public void setEmail(String email) {
        this.email = email;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Date getBD() {
        return bD;
    }

    public void setBD(Date bD) {
        this.bD = bD;
    }

    public Date getRegDate() {
        return regDate;
    }

    public void setRegDate(Date regDate) {
        this.regDate = regDate;
    }

    public Integer getCMemberID() {
        return cMemberID;
    }

    public void setCMemberID(Integer cMemberID) {
        this.cMemberID = cMemberID;
    }

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public String getReceiverAddress() {
        return receiverAddress;
    }

    public void setReceiverAddress(String receiverAddress) {
        this.receiverAddress = receiverAddress;
    }

    public String getReceiverPhone() {
        return receiverPhone;
    }

    public void setReceiverPhone(String receiverPhone) {
        this.receiverPhone = receiverPhone;
    }

    public String getCardName() {
        return cardName;
    }

    public void setCardName(String cardName) {
        this.cardName = cardName;
    }

    public String getCardValidTime() {
        return cardValidTime;
    }

    public void setCardValidTime(String cardValidTime) {
        this.cardValidTime = cardValidTime;
    }

    public String getCardLast3No() {
        return cardLast3No;
    }

    public void setCardLast3No(String cardLast3No) {
        this.cardLast3No = cardLast3No;
    }

    public String getCardPhone() {
        return cardPhone;
    }

    public void setCardPhone(String cardPhone) {
        this.cardPhone = cardPhone;
    }

    @Override
    public String toString() {
        return "Member{" +
                "memberID=" + memberID +
                ", personalPhotos=" + Arrays.toString(personalPhotos) +
                ", name='" + name + '\'' +
                ", account='" + account + '\'' +
                ", password='" + password + '\'' +
                ", email='" + email + '\'' +
                ", gender='" + gender + '\'' +
                ", phone='" + phone + '\'' +
                ", address='" + address + '\'' +
                ", bD=" + bD +
                ", regDate=" + regDate +
                ", cMemberID=" + cMemberID +
                ", receiver='" + receiver + '\'' +
                ", receiverAddress='" + receiverAddress + '\'' +
                ", receiverPhone='" + receiverPhone + '\'' +
                ", cardName='" + cardName + '\'' +
                ", cardValidTime='" + cardValidTime + '\'' +
                ", cardLast3No='" + cardLast3No + '\'' +
                ", cardPhone='" + cardPhone + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Member member = (Member) o;
        return Objects.equals(memberID, member.memberID) && Arrays.equals(personalPhotos, member.personalPhotos) && Objects.equals(name, member.name) && Objects.equals(account, member.account) && Objects.equals(password, member.password) && Objects.equals(email, member.email) && Objects.equals(gender, member.gender) && Objects.equals(phone, member.phone) && Objects.equals(address, member.address) && Objects.equals(bD, member.bD) && Objects.equals(regDate, member.regDate) && Objects.equals(cMemberID, member.cMemberID) && Objects.equals(receiver, member.receiver) && Objects.equals(receiverAddress, member.receiverAddress) && Objects.equals(receiverPhone, member.receiverPhone) && Objects.equals(cardName, member.cardName) && Objects.equals(cardValidTime, member.cardValidTime) && Objects.equals(cardLast3No, member.cardLast3No) && Objects.equals(cardPhone, member.cardPhone);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(memberID, name, account, password, email, gender, phone, address, bD, regDate, cMemberID, receiver, receiverAddress, receiverPhone, cardName, cardValidTime, cardLast3No, cardPhone);
        result = 31 * result + Arrays.hashCode(personalPhotos);
        return result;
    }
}