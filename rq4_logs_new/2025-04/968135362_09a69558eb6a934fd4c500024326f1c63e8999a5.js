(function ()
 { var run = function () 
     { if (typeof window.LadiPageScript == "undefined" || typeof window.ladi == "undefined" || window.ladi == undefined)
      { setTimeout(run, 100); return; }
       window.LadiPageApp = window.LadiPageApp || new window.LadiPageAppV2(); window.LadiPageScript.runtime.ladipage_id = '645e1049954d0e001291aa9f';
        window.LadiPageScript.runtime.publish_platform = 'LADIPAGE'; window.LadiPageScript.runtime.version = '1687243116828'; 
        window.LadiPageScript.runtime.cdn_url = 'https://w.ladicdn.com/v2/source/'; 
        window.LadiPageScript.runtime.DOMAIN_FREE = ["preview.ladipage.me", "ldp.link", "ldp.page"]; 
        window.LadiPageScript.runtime.bodyFontSize = 12; window.LadiPageScript.runtime.store_id = "5f5ee29e7d8d6832b5e05ec9"; 
        window.LadiPageScript.runtime.time_zone = 7; window.LadiPageScript.runtime.currency = "VND"; 
        window.LadiPageScript.runtime.convert_replace_str = true; 
        window.LadiPageScript.runtime.desktop_width = 1200; 
        window.LadiPageScript.runtime.mobile_width = 420;
         window.LadiPageScript.runtime.formdata = true;
          window.LadiPageScript.runtime.tracking_button_click = true; 
          window.LadiPageScript.runtime.lang = "vi"; 
          window.LadiPageScript.run(true); 
          window.LadiPageScript.runEventScroll(); }; 
          run(); })
          ();