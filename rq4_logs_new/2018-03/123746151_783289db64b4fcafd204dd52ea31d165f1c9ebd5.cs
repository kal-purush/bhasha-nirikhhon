using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

/// <summary>
/// Summary description for Project
/// </summary>
public class Project
{
    private string title;
    private string imageurl;
    private string description;

    public string m_Title
    {
        get
        {
            return title;
        }
        set
        {
            title = value;
        }
    }
    public string m_Imageurl
    {
        get
        {
            return imageurl;
        }
        set
        {
            imageurl = value;
        }
    }
    public string m_Description
    {
        get
        {
            return description;
        }
        set
        {
            description = value;
        }
    }
    public Project(string title,string description,string imageurl)
    {
        //
        // TODO: Add constructor logic here
        //
        m_Title = title;
        m_Description = description;
        m_Imageurl = imageurl;
    }
}