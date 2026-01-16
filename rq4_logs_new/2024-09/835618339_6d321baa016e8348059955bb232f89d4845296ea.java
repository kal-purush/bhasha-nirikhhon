package com.kh.erp.VO;

import java.sql.Date;

import lombok.Data;

//보고서 기록조회
@Data
public class TbReportRecVO {
	private Date writeDate; // 휴가신청일 --> write_Date
	private String reportTitle; // 휴가신청서 제목 //임시
	private String wirterId; // 작성자 ID writer_Id
	private String approBosName; // 결재자 이름
	private String approBosId;// 결재자 Id
	private String approYN; // 결재여부 (Y/N)
	private int approNo; //appro_No
	private int reportNo; //report_No
	private String writerDept; // writer_Dept
	private String writerName; // writer_Name
}
