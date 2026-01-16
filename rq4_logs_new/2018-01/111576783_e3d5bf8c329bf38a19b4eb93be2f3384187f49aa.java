package fr.litarvan.sakado.server.push;

import java.util.ArrayList;
import java.util.List;

public class PushInfo
{
    private String deviceToken;
    private List<String> sent;

    public PushInfo(String deviceToken)
    {
        this.deviceToken = deviceToken;
        this.sent = new ArrayList<>();
    }

    public String getDeviceToken()
    {
        return deviceToken;
    }

    public boolean isToSend(String message)
    {
        return this.deviceToken != null && !this.sent.contains(message);
    }

    public void sent(String message)
    {
        this.sent.add(message);
    }
}