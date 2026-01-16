package com.tia102g3.coachcertificate.model;


import javax.persistence.*;
import java.util.Arrays;
import java.util.Objects;

/**
 * ClassName： CoachCertificate
 * package：com.tia102g3.coachcertificate.model
 * Description：
 *
 * @Author 任少騫
 * @Create 2024/7/18 @{TIME}
 * @Version 1.0
 */
@Entity
@Table(name = "coach_certificate")
public class CoachCertificate {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "coachCertificateID", updatable = false)
    private Integer coachCertificateID;
    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "cMemberID", referencedColumnName = "cMemberID", nullable = false)
    private CMember cMember;
    @Column(name = "certificateName", nullable = false, length = 200)
    private String certificateName;
    @Column(name = "certificatePic", columnDefinition = "LONGBLOB"  )
    private byte[] certificatePic;

    public CoachCertificate() {
    }

    public CoachCertificate(Integer coachCertificateID, CMember cMember, String certificateName, byte[] certificatePic) {
        this.coachCertificateID = coachCertificateID;
        this.cMember = cMember;
        this.certificateName = certificateName;
        this.certificatePic = certificatePic;
    }

    public Integer getCoachCertificateID() {
        return coachCertificateID;
    }

    public void setCoachCertificateID(Integer coachCertificateID) {
        this.coachCertificateID = coachCertificateID;
    }

    public CMember getCMember() {
        return cMember;
    }

    public void setCMember(CMember cMember) {
        this.cMember = cMember;
    }

    public String getCertificateName() {
        return certificateName;
    }

    public void setCertificateName(String certificateName) {
        this.certificateName = certificateName;
    }

    public byte[] getCertificatePic() {
        return certificatePic;
    }

    public void setCertificatePic(byte[] certificatePic) {
        this.certificatePic = certificatePic;
    }

    @Override
    public String toString() {
        return "CoachCertificate{" +
                "coachCertificateID=" + coachCertificateID +
                ", cMember=" + cMember +
                ", certificateName='" + certificateName + '\'' +
                ", certificatePic=" + Arrays.toString(certificatePic) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoachCertificate that = (CoachCertificate) o;
        return Objects.equals(coachCertificateID, that.coachCertificateID) && Objects.equals(cMember, that.cMember) && Objects.equals(certificateName, that.certificateName) && Objects.deepEquals(certificatePic, that.certificatePic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coachCertificateID, cMember, certificateName, Arrays.hashCode(certificatePic));
    }
}