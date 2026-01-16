## Constanten:
## Sysordner mit Daten für Popup
## popupPid

## Column in Sysordner für Popupdaten
## popupColPos

## Shadowboxadapter 
## popupShadowboxAdapter
## 
## Verfügbare ShadowboxAdapter:
## - default (shadowbox)
## - colorbox

## Integration auf Webseite
#<INCLUDE_TYPOSCRIPT: source="FILE: fileadmin/layout/ts_popup.ts">
#
## Beispielintegration mit Global Condition
# [globalVar = GP:L = 1] && [globalVar = TSFE:id = 1]
# page.footerData.666 < temp.popup
# page.footerData.666.4.value (

##   Höhe des Poups
#   var objHeight = 500;

##  Breite des Popups
#   var objWidth = 800;

##  Soll cookie für Session verwendet werden?
#   var setCookie = 0;

##   Name des Cookies (für Mehrere Popups auf Seite)
#   var cookieName = "popupStartseite";
#)
#[global]

## Popup Integration
temp.popup = COA
temp.popup {
	1 = CONTENT
	1 {
		table = tt_content
		select.max = 1
		select.orderBy = sorting
		select.pidInList = {$popupPid}
		select.where = ColPos = {$popupColPos}
		select.languageField = sys_language_uid   
		wrap = <div id="popup" style="display:none"><div style="color:#fff; padding:25px;" class="poupcontent"|</div></div>
	}

	3 = TEXT
	3.value (
    	<script type="text/javascript"> 
		$(document).ready(function () {
	)
	
	4 = TEXT
	4.value (
		var objHeight = 725;
		var objWidth = 1000;
		var cookieName = "popup";
		var setCookie = 0;
	)
  
	5 = CASE
	5.key = {$popupShadowboxAdapter}
	
	5.default = TEXT
	5.default.value (
    	if( !jQuery.cookie || !jQuery.cookie(cookieName)) {
			setTimeout(function() {
				Shadowbox.open({         
				content: jQuery("#popup").html(),
				player: "html",         
				title: "",         
				height: objHeight,         
				width: objWidth
			  });   
		    }, 2000); 
     
		    var css = '#sb-player.html { background: lightgray; } #sb-player .csc-textpic-image { margin-bottom: 0 !important; }',     
		    head = document.getElementsByTagName('head')[0],    
		    style = document.createElement('style');  
		    style.type = 'text/css'; 
		    if (style.styleSheet) {   
		    	style.styleSheet.cssText = css; 
		    } else {   
		    	style.appendChild(document.createTextNode(css)); 
		    }  
		    head.appendChild(style);     
		     
		    if(setCookie)
		    	jQuery.cookie(cookieName, "true");
		}
	  });
	  </script>
	)
	
	5.colorbox = TEXT
	5.colorbox.value (
		if( !jQuery.cookie || !jQuery.cookie(cookieName)) {
			setTimeout(function() {
				jQuery.colorbox({         
				html: jQuery("#popup").html(),
				height: objHeight,         
				width: objWidth
			  });   
		    }, 2000); 
     		     
		    if(setCookie)
		    	jQuery.cookie(cookieName, "true");
		}
	  });
	  </script>	
	)
	
	99 = TEXT
	99.value (
	  sb-body .csc-textpic-imagewrap {width: 100% !important;}
      sb-body img {height: auto; margin-bottom: 0; width: 100%;}
      sb-body .csc-space {margin:0}
    )
    99.wrap = <style>|</style>
}