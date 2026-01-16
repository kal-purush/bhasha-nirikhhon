package com.hopper.model.availability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "AvailabilityLongResponseV2")
@XmlAccessorType(XmlAccessType.FIELD)
public class AvailabilityResponse
{
    @XmlAttribute(name = "status")
    private int status;

    public int getStatus()
    {
        return status;
    }

    public void setStatus(int status)
    {
        this.status = status;
    }
}