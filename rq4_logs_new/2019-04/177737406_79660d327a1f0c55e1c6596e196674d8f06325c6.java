package com.lavenir.communication.data;

import lombok.Data;

@Data
public class MinaProtocolData {
    private String protocolId;
    private String signature;
    private String content;
    private String clientCode;
}