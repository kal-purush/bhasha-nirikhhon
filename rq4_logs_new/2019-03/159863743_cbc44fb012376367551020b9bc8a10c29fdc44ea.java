package com.university.contractors.model;

import com.google.common.base.Objects;

import javax.persistence.*;
import java.util.Date;

@Entity
public class Contract implements IdEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_contract")
    private Long id;

    @ManyToOne
    @JoinColumn(name = "ref_student")
    private Student student;

    @ManyToOne
    @JoinColumn(name = "ref_contract_type")
    private ContractType contractType;
    private Date contractDate;
    private Float contractValue;
    private String contractNumber;

    @ManyToOne
    @JoinColumn(name = "ref_order_project")
    private Order order;

    @ManyToOne
    @JoinColumn(name = "ref_dedact_order_project")
    private Order deactOrder;

    @ManyToOne
    @JoinColumn(name = "ref_educ_prog")
    private EducationProgram educationProgram;

    @ManyToOne
    @JoinColumn(name = "ref_educ_level")
    private EducationLevel educationLevel;

    @ManyToOne
    @JoinColumn(name = "ref_educ_form")
    private EducationForm educationForm;

    @ManyToOne
    @JoinColumn(name = "ref_arrival_line")
    private ArrivalLine arrivalLine;

    private String payer;
    private Date dateIn;
    private Date planDateOut;
    private Integer course;

    @ManyToOne
    @JoinColumn(name = "ref_educ_language")
    private EducationLanguage educationLanguage;

    @ManyToOne
    @JoinColumn(name = "ref_student_status")
    private StudentStatus studentStatus;

    private Float yearsOfEduc;
    private String comment;

    @ManyToOne
    @JoinColumn(name = "ref_usual_payment_form")
    private PaymentForm usualPaymentForm;

    @ManyToOne
    @JoinColumn(name = "ref_direction")
    private Direction direction;

    private Boolean isBudget;
    private Boolean isActive;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public Student getStudent() {
        return student;
    }

    public void setStudent(Student student) {
        this.student = student;
    }

    public ContractType getContractType() {
        return contractType;
    }

    public void setContractType(ContractType contractType) {
        this.contractType = contractType;
    }

    public Date getContractDate() {
        return contractDate;
    }

    public void setContractDate(Date contractDate) {
        this.contractDate = contractDate;
    }

    public Float getContractValue() {
        return contractValue;
    }

    public void setContractValue(Float contractValue) {
        this.contractValue = contractValue;
    }

    public String getContractNumber() {
        return contractNumber;
    }

    public void setContractNumber(String contractNumber) {
        this.contractNumber = contractNumber;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public Order getDeactOrder() {
        return deactOrder;
    }

    public void setDeactOrder(Order deactOrder) {
        this.deactOrder = deactOrder;
    }

    public EducationProgram getEducationProgram() {
        return educationProgram;
    }

    public void setEducationProgram(EducationProgram educationProgram) {
        this.educationProgram = educationProgram;
    }

    public EducationLevel getEducationLevel() {
        return educationLevel;
    }

    public void setEducationLevel(EducationLevel educationLevel) {
        this.educationLevel = educationLevel;
    }

    public EducationForm getEducationForm() {
        return educationForm;
    }

    public void setEducationForm(EducationForm educationForm) {
        this.educationForm = educationForm;
    }

    public ArrivalLine getArrivalLine() {
        return arrivalLine;
    }

    public void setArrivalLine(ArrivalLine arrivalLine) {
        this.arrivalLine = arrivalLine;
    }

    public String getPayer() {
        return payer;
    }

    public void setPayer(String payer) {
        this.payer = payer;
    }

    public Date getDateIn() {
        return dateIn;
    }

    public void setDateIn(Date dateIn) {
        this.dateIn = dateIn;
    }

    public Date getPlanDateOut() {
        return planDateOut;
    }

    public void setPlanDateOut(Date planDateOut) {
        this.planDateOut = planDateOut;
    }

    public Integer getCourse() {
        return course;
    }

    public void setCourse(Integer course) {
        this.course = course;
    }

    public EducationLanguage getEducationLanguage() {
        return educationLanguage;
    }

    public void setEducationLanguage(EducationLanguage educationLanguage) {
        this.educationLanguage = educationLanguage;
    }

    public StudentStatus getStudentStatus() {
        return studentStatus;
    }

    public void setStudentStatus(StudentStatus studentStatus) {
        this.studentStatus = studentStatus;
    }

    public Float getYearsOfEduc() {
        return yearsOfEduc;
    }

    public void setYearsOfEduc(Float yearsOfEduc) {
        this.yearsOfEduc = yearsOfEduc;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public PaymentForm getUsualPaymentForm() {
        return usualPaymentForm;
    }

    public void setUsualPaymentForm(PaymentForm usualPaymentForm) {
        this.usualPaymentForm = usualPaymentForm;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public Boolean getBudget() {
        return isBudget;
    }

    public void setBudget(Boolean budget) {
        isBudget = budget;
    }

    public Boolean getActive() {
        return isActive;
    }

    public void setActive(Boolean active) {
        isActive = active;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Contract contract = (Contract) o;
        return Objects.equal(id, contract.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}