package com.quantumai.customer.dto;

import java.time.LocalDateTime;
import lombok.Data;

@Data
public class OTPEntry {

  private String otp;
  private LocalDateTime createTime;
}