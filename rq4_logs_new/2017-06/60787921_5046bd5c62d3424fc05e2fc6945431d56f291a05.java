package dataBean.dubbo.projectSys;

import dataBean.IDataBean;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by alex on 2017/5/31.
 */
public class MyContractDto implements Serializable , IDataBean{

    private String id;

    private static final long serialVersionUID = -5264130894983620291L;
    /**
     * 项目编号
     */
    private String projectCode;
    /**
     * 合同名称
     */
    private String contractName;
    /**
     * 原合同编号
     */
    private String originalContractCode;
    /**
     *剩余金额
     */
    private Double residualAmount;
    /**
     * 合同开始时间
     */
    private Date contractStartDate;
    /**
     * 合同结束时间
     */
    private Date contractEndDate;
    /**
     * 原债权方
     */
    private String originalCreditor;
    /**
     * 债务方
     */
    private String debtor;


    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    /**
     * 产品id
     * @return
     */
    public String getProjectCode() {
        return projectCode;
    }
    /**
     * 产品id
     * @return
     */
    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }

    /**
     * 合同名称
     * @return
     */
    public String getContractName() {
        return contractName;
    }

    /**
     * 合同名
     * @param contractName
     */
    public void setContractName(String contractName) {
        this.contractName = contractName;
    }

    /**
     * 原合同编号
     * @return
     */
    public String getOriginalContractCode() {
        return originalContractCode;
    }

    /**
     * 原合同编号
     * @param originalContractCode
     */
    public void setOriginalContractCode(String originalContractCode) {
        this.originalContractCode = originalContractCode;
    }


    /**
     * 合同开始时间
     * @return
     */
    public Date getContractStartDate() {
        return contractStartDate;
    }

    /**
     *
     * 合同开始时间
     * @param contractStartDate
     */
    public void setContractStartDate(Date contractStartDate) {
        this.contractStartDate = contractStartDate;
    }

    /**
     * 合同结束时间
     * @return
     */
    public Date getContractEndDate() {
        return contractEndDate;
    }

    /**
     * 合同结束时间
     * @param contractEndDate
     */
    public void setContractEndDate(Date contractEndDate) {
        this.contractEndDate = contractEndDate;
    }

    /**
     * 原债权方
     * @return
     */
    public String getOriginalCreditor() {
        return originalCreditor;
    }

    /**
     * 原债权方
     * @param originalCreditor
     */
    public void setOriginalCreditor(String originalCreditor) {
        this.originalCreditor = originalCreditor;
    }

    /**
     * 债务方
     * @return
     */
    public String getDebtor() {
        return debtor;
    }

    /**
     * 债务方
     * @param debtor
     */
    public void setDebtor(String debtor) {
        this.debtor = debtor;
    }

    public Double getResidualAmount() {
        return residualAmount;
    }

    public void setResidualAmount(Double residualAmount) {
        this.residualAmount = residualAmount;
    }


    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ContractDto{" +
                "projectCode='" + projectCode + '\'' +
                ", contractName='" + contractName + '\'' +
                ", originalContractCode='" + originalContractCode + '\'' +
                ", residualAmount=" + residualAmount +
                ", contractStartDate=" + contractStartDate +
                ", contractEndDate=" + contractEndDate +
                ", originalCreditor='" + originalCreditor + '\'' +
                ", debtor='" + debtor + '\'' +
                '}';
    }
}