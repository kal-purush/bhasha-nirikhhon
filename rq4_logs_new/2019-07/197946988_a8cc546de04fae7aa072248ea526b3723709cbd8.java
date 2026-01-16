package com.ef.domain;

import lombok.Data;

@Data
public class AccessIpStatistics {
    private String ip;
    private Long ipCount;

    public AccessIpStatistics(String ip, Long ipCount) {
        this.ip = ip;
        this.ipCount = ipCount;
    }
}