package com.example.work.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Log {
    @AutoConfigureOrder
    private String ip;
    private String date;
    private String method;
    private String url;
    private String protocol;
    private String time;
    private String status;

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}