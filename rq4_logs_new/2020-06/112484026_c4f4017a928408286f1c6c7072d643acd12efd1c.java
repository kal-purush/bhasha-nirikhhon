package com.cloudesire.platform.apiclient.dto.model.dto.csv;

import com.cloudesire.platform.apiclient.dto.model.dto.DTO;

abstract class BaseUserCsvDTO extends DTO
{
    private String name;

    private String surname;

    private String phone;

    private String email;

    private String pec;

    // region Auto-generated code
    public String getName()
    {
        return name;
    }

    public void setName( String name )
    {
        this.name = name;
    }

    public String getSurname()
    {
        return surname;
    }

    public void setSurname( String surname )
    {
        this.surname = surname;
    }

    public String getPhone()
    {
        return phone;
    }

    public void setPhone( String phone )
    {
        this.phone = phone;
    }

    public String getEmail()
    {
        return email;
    }

    public void setEmail( String email )
    {
        this.email = email;
    }

    public String getPec()
    {
        return pec;
    }

    public void setPec( String pec )
    {
        this.pec = pec;
    }
    // endregion

    abstract static class BaseCompany
    {
        private String name;

        private String vat;

        private String country;

        private String address;

        private String city;

        private String state;

        private String zip;

        private String geocallId;

        private String sdiCode;

        private String pec;

        private String type;

        public String getName()
        {
            return name;
        }

        public void setName( String name )
        {
            this.name = name;
        }

        public String getVat()
        {
            return vat;
        }

        public void setVat( String vat )
        {
            this.vat = vat;
        }

        public String getCountry()
        {
            return country;
        }

        public void setCountry( String country )
        {
            this.country = country;
        }

        public String getAddress()
        {
            return address;
        }

        public void setAddress( String address )
        {
            this.address = address;
        }

        public String getCity()
        {
            return city;
        }

        public void setCity( String city )
        {
            this.city = city;
        }

        public String getState()
        {
            return state;
        }

        public void setState( String state )
        {
            this.state = state;
        }

        public String getZip()
        {
            return zip;
        }

        public void setZip( String zip )
        {
            this.zip = zip;
        }

        public String getGeocallId()
        {
            return geocallId;
        }

        public void setGeocallId( String geocallId )
        {
            this.geocallId = geocallId;
        }

        public String getSdiCode()
        {
            return sdiCode;
        }

        public void setSdiCode( String sdiCode )
        {
            this.sdiCode = sdiCode;
        }

        public String getPec()
        {
            return pec;
        }

        public void setPec( String pec )
        {
            this.pec = pec;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }
    }
}