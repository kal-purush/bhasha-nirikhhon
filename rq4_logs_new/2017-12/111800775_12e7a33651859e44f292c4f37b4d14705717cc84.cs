        public Boolean InsertHits(int id, string type)
        {
            return BllSmart.InsertHits(id, type);
        }
        public Boolean InsertLocation(int id, String user, int duree)
        {
            return BllSmart.InsertLocation(id, user, duree);
        }
            List<FilmDTO> list = new List<FilmDTO>();
            list = service.GetFilmByName(name).ToList();
            foreach(FilmDTO f in list)
            {
                service.InsertHits(f.Id,"Film");
            }
            return list;
            List<ActeurDTO> list = new List<ActeurDTO>();
            list = service.GetActorByName(name).ToList();
            foreach (ActeurDTO f in list)
            {
                service.InsertHits(f.Id, "Acteur");
            }
            return list;
        public Boolean InsertLocation(int id,String user,int duree)
        {
            return service.InsertLocation(id,user,duree);
        }

        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IService1/InsertHits", ReplyAction="http://tempuri.org/IService1/InsertHitsResponse")]
        bool InsertHits(int id, string type);
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IService1/InsertHits", ReplyAction="http://tempuri.org/IService1/InsertHitsResponse")]
        System.Threading.Tasks.Task<bool> InsertHitsAsync(int id, string type);
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IService1/InsertLocation", ReplyAction="http://tempuri.org/IService1/InsertLocationResponse")]
        bool InsertLocation(int id, string user, int duree);
        
        [System.ServiceModel.OperationContractAttribute(Action="http://tempuri.org/IService1/InsertLocation", ReplyAction="http://tempuri.org/IService1/InsertLocationResponse")]
        System.Threading.Tasks.Task<bool> InsertLocationAsync(int id, string user, int duree);
        
        public bool InsertHits(int id, string type) {
            return base.Channel.InsertHits(id, type);
        }
        
        public System.Threading.Tasks.Task<bool> InsertHitsAsync(int id, string type) {
            return base.Channel.InsertHitsAsync(id, type);
        }
        
        public bool InsertLocation(int id, string user, int duree) {
            return base.Channel.InsertLocation(id, user, duree);
        }
        
        public System.Threading.Tasks.Task<bool> InsertLocationAsync(int id, string user, int duree) {
            return base.Channel.InsertLocationAsync(id, user, duree);
        }


        protected void ButtonTrailer_Click(object sender, EventArgs e)
        {
            if(film.Trailer != null)
            {
                System.Diagnostics.Process.Start(film.Trailer);
            }
        }

        protected void ButtonLocationFilm_Click(object sender, EventArgs e)
        {
            Server.Transfer("LocationFilm.aspx?titre=" + int.Parse(Request.QueryString["titre"]), true);
        }
        
        /// <summary>
        /// ButtonTrailer control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Button ButtonTrailer;
        
        /// <summary>
        /// ButtonLocationFilm control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Button ButtonLocationFilm;
﻿using Web_SmartVidéo.ServiceReference1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;

namespace Web_SmartVidéo
{
    public partial class LocationFilm : Page
    {
        FilmDTO film;
        private AuthenticationControler aC;
        protected void Page_Load(object sender, EventArgs e)
        {
            aC = new AuthenticationControler();
            int id =int.Parse(Request.QueryString["titre"]);
            if (Session["Log"] != null && Session["LogOK"] != null)
            {
                HtmlAnchor link = (HtmlAnchor)this.Master.FindControl("Log");
                link.InnerText = (String)Session["Log"];
                link.HRef = (String)Session["LogOK"];
            }
            film = aC.GetFilm(id);
            Label1.Text = film.Title;
        }

        protected void Button1_Click(object sender, EventArgs e)
        {
            int duree;
            bool result = int.TryParse(TextBoxDuree.Text,out duree);
            if (result)
            {
                //ok pour format
                if (duree > 0 && duree < 12)
                {
                    //ok pour durée
                    if(aC.InsertLocation(film.Id, (String)Session["Log"], duree))
                    {
                        Server.Transfer("Default.aspx", true);
                    }
                }
                else
                    TextBoxDuree.Text = "3";
            }
        }
    }
}
﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated. 
// </auto-generated>
//------------------------------------------------------------------------------

namespace Web_SmartVidéo {
    
    
    public partial class LocationFilm {
        
        /// <summary>
        /// Label1 control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Label Label1;
        
        /// <summary>
        /// Label2 control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Label Label2;
        
        /// <summary>
        /// TextBoxDuree control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.TextBox TextBoxDuree;
        
        /// <summary>
        /// Label3 control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Label Label3;
        
        /// <summary>
        /// Button1 control.
        /// </summary>
        /// <remarks>
        /// Auto-generated field.
        /// To modify move field declaration from designer file to code-behind file.
        /// </remarks>
        protected global::System.Web.UI.WebControls.Button Button1;
    }
}