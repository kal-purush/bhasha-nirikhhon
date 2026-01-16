/**************************************************************************************
Siteways Banner Script.
A script to read mutiple sources and replace banners setup with <INS tags
or to do inline replaces.
Ver: .01 - Do some setup for class etc..
**************************************************************************************/
let Siteways = {
  Globals: {
    RootApi: "https://app.swinity.com/api",
    LogEnabled: true,
    BannerSpots: [],
  },
  Log: (txt,obj) => {
    if(Siteways.Globals.LogEnabled) {
      if(typeof obj === "undefined") {
        console.log(txt);
      } else {
        console.log(txt,obj);
      }
    }
  },
  MakeId: (length) => {
    let result = '';
    let characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let charactersLength = characters.length;
    for ( let i = 0; i < length; i++ ) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
  },

  /**********************************************************************************
   The Below Functions Allow Me To Call HTTP Based APIS Without JQuery All Callz
   Are Supported Down To IE 3.0, And Any Other Browser In The Last 20 Years :)
   **********************************************************************************/
  HttpGet: (url,headers,cb,pb) => {
    try {
      let xhr = new XMLHttpRequest();
      xhr.open("GET",url);
      if(typeof headers !== "undefined") {
        for(let k in headers) {
          xhr.setRequestHeader(k,headers[k]);
        }
      }
      xhr.onload = () => {
        if(xhr.status === 200) {
          let obj = JSON.parse(xhr.responseText);
          cb(obj,pb);
        } else {
          try { //Try To Read The JSON Output If Any
            let obj = JSON.parse(xhr.responseText);
            cb(obj,pb);
          } catch (ex) {}
          cb({Code: xhr.status, Message: xhr.statusText},pb);
        }
      };
      xhr.send();
    } catch(ex) {
      console.error("Error in Siteways.Globals.HttpGet: " + ex.toString());
    }
  },
  Banners: {
    //Function to mimic the $(document).ready() functionality of jQuery
    OnReady: (cb) => {
      if (document.readyState!='loading') cb();
      // modern browsers
      else if (document.addEventListener) document.addEventListener('DOMContentLoaded', cb);
      else document.attachEvent('onreadystatechange', function(){
          if (document.readyState=='complete') cb();
        });
    },
    //This function loads the the page tags, and the banners for the spots
    Init: () => {
      try {
        Siteways.Globals.BannerSpots = [];
        Siteways.Banners.PlaceBanners();
      } catch(ex) {
        console.error("Error in Siteways.Banners.Init: " + ex.toString());
      }
    },

    //This Is Function To Call To Set The Current Banners
    PlaceBanners: () => {


      //Always Reload, Maybe They Added New :)
      Siteways.Globals.BannerSpots = [];
      Siteways.Globals.BannerSpots = Siteways.Banners.GetInsertTags();

      Siteways.Globals.BannerSpots.forEach((s) => {
        let ele = s.Element;
        ele.setAttribute("style","display: none");
        let headers = {
          "X-Alt-Referer": document.location.href
        };
        Siteways.HttpGet(Siteways.Globals.RootApi + `/creatives/${s.Id}?channel=${s.Channel}&max=${s.Count}&minAdvertiseHere=${s.MinBuyAds}&maxAdvertiseHere=${s.MaxBuyAds}`,headers,(data,pb) => {



          if(data.Code === 200) {


            data.Result.forEach((v,i) => {
              let rec = v.Creative;
              if(rec.Code == "ADVERTISEHERE") {
                let div = document.createElement("div");
                div.id=s.DomId + "-" + rec.Code;
                div.className=pb.Class + " site-banner-spot";
                let ext = document.getElementById(div.id);
                if(ext !== null) {
                  pb.Element.parentNode.removeChild(ext);
                }
                pb.Element.parentNode.appendChild(div);
                try {
                  div.innerHTML = document.getElementById(s.BuyAdTemplate).innerHTML;
                } catch(ex) {}
              } else {
                let href = document.createElement("a");
                let img = document.createElement("img");
                href.id=s.DomId + "-" + rec.Code; href.href=rec.TargetUrl; href.target="_blank"; href.title=rec.Title; href.className=pb.Class + " site-banner-spot";
                img.src=rec.Url; img.alt=rec.Title;
                let ext = document.getElementById(href.id);
                if(ext !== null) {
                  pb.Element.parentNode.replaceChild(href,ext);
                } else {
                  pb.Element.parentNode.appendChild(href);
                }
                href.appendChild(img);
                pb.Element.parentNode.appendChild(href);
              }
            });

            //Remove any previous banners :) On Redo :)
            let col = document.getElementsByClassName("site-banner-spot");
            let eles = Array.from(col);
            for(let xInt=0; xInt<=eles.length-1; xInt++) {
              let nId = eles[xInt].id;
              let rem = true;
              data.Result.forEach((v,i) => {
                let cId = s.DomId + "-" + v.Creative.Code;
                if(cId == nId) {
                  rem=false;
                }
              });
              if(rem) {
                eles[xInt].parentNode.removeChild(eles[xInt]);
              }
            }
          }
        },s);
      });
    },



    GetAttributeString(obj,nme) {
      let t = obj.getAttribute(nme);
      return t === null ? "" : t;
    },

    //Function to get a list of insert tags using only standard JS callz, works down to IE 3.0 browsers(WIN 95)
    GetInsertTags: () => {
      let spots = [];
      try {
        let eles = document.getElementsByTagName("ins");
        for(let x=0; x<=eles.length-1; x++) {
          let hObj = eles.item(x);
          let tObj = {
            Element: hObj,
            Id:  Siteways.Banners.GetAttributeString(hObj,"data-key"),
            Channel: Siteways.Banners.GetAttributeString(hObj,"data-channel"),
            Class: Siteways.Banners.GetAttributeString(hObj,"data-class"),
            Count: Siteways.Banners.GetAttributeString(hObj,"data-count"),
            MinBuyAds: isNaN(Siteways.Banners.GetAttributeString(hObj,"data-minbuyads")) ? "0" : Siteways.Banners.GetAttributeString(hObj,"data-minbuyads"),
            MaxBuyAds: isNaN(Siteways.Banners.GetAttributeString(hObj,"data-maxbuyads")) ? "0" : Siteways.Banners.GetAttributeString(hObj,"data-maxbuyads"),
            Country: Siteways.Banners.GetAttributeString(hObj,"data-country"),
            BuyAdTemplate: Siteways.Banners.GetAttributeString(hObj,"data-buyadtemplate"),
            DomId: Siteways.Banners.GetAttributeString(hObj,"data-domid") == "" ? Siteways.MakeId(15) : Siteways.Banners.GetAttributeString(hObj,"data-domid"),
          };
          hObj.setAttribute("data-domid",tObj.DomId);
          spots.push(tObj);
        }
      } catch(ex) {
        console.error("Error in Siteways.Banners.GetInsertTags: " + ex.toString());
        throw ex;
      }
      return spots;
    },
  }



};




Siteways.Banners.OnReady(()=> {
  Siteways.Banners.Init(); //Call All Needed Jazz :)
});


