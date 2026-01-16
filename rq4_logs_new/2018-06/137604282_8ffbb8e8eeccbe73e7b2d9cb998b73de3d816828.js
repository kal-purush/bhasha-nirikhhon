// �������� JS �������

  /*
    ��� ������ �� �������� ���� ���������� ��� ������ ���������
    (� ����������� onUnload ������� DestroyAllViewers())
  */
  window.onunload = DestroyAllViewers;

  // ������ �����������������
  var rmxfViewersVid = new Array();
  var rmxfViewerSnd = null;

  function CutProto(url)
  {
    var re = new RegExp("^[a-z0-9]+://", "i");
    var reres = re.exec(url);
    if (reres == null) return url;
    return RegExp.rightContext;
  }

  function SetPort(url)
  {
    var pos1=url.indexOf(':');
    var pos2=url.indexOf('/');
    url = url.substring(0,pos1+1) + port + url.substring(pos2);

    return url;
  }

  function GetNormalUrl(url)
  {
    CutURL = CutProto(url);

    if (port != 'DEFAULT')
      CutURL = SetPort(CutURL);

    if (protocol == 'VDP')
      url = 'rmxvp://' + CutURL;
    else
    if (protocol == 'TCP')
      url = 'rmxsp://' + CutURL;
    else
    if (protocol == 'VCP')
      url = 'rmxsp://' + CutURL;
    else
    if ((protocol == 'UDP') || (protocol == 'UDPU'))
      url = 'rmxdp://' + CutProto(url);
    else
    if (protocol == 'UDPM')
    {
      url = 'rmxdp://' + CutURL;
      this.tryMulticast = 1;
    }
    else
    { proto = url.substring(0, url.indexOf('://')+3);
      url = proto + CutURL;
      if ((proto == 'rmxdp://') || (proto == 'RMXDP://'))
      this.tryMulticast = 1;
    }

    return url;
  }

  function startRecord()
  {
    recording = 1 - recording;
    if (recording == 1) 
{   document.getElementById('idRecord').value = 'Stop  Rec';
    document.getElementById('idRecord').style.backgroundColor='#ff5555';
}
    else
{   document.getElementById('idRecord').value = 'Start Rec';
    document.getElementById('idRecord').style.backgroundColor='#88ff88';
}
  }


//StartPositionMs=-1
//StopPositionMs=-1


  // ������������ ������ �� ��������� ��������
//alert('processing playvideo');
  function playVideo(URLBaseIndex, index, viewM)
  {

    viewMethod = viewM;

    currentRef = index;
    enableAudio = 1;
    if (refLang.length > 1)
    {
    if (refLangUrl[currentLang] != "")
           enableAudio = 1; // ��������� ���� � �������� ���� (�����), ���� ��� �������
    }
    
    var el = 0;
    el = document.getElementById('recvProtocol');
    if (el) protocol  = el.value;
    el = document.getElementById('recvPort');
    if (el) port = el.value;
    el = document.getElementById('recvbufferMid');
    if (el) bufferMid = el.value;

    var refUrls = refUrl[index].split(' ');     // ====================

    var url = new Array();
    for (var i = 0; i < refUrls.length; i++)
    {
      if (refUrls[i].indexOf('*') == 0) // ���� url ���������� � ������� "*", �� �� ������������ ������������ !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        url[i] = GetNormalUrl(refUrls[i].substr(1));
      else
        url[i] = GetNormalUrl(URLBase[URLBaseIndex]+refUrls[i]);
    }

    var urlSnd = GetNormalUrl(URLBase[URLBaseIndex]+refLangUrl[currentLang]);

    var tryMulticast = false; // ???


    var bufferDel = bufferDev;
    var bufferMin = parseInt(bufferMid)-bufferDel;
    var bufferMax = parseInt(bufferMid)+parseInt(bufferDel);

    for (var i = 0; i < rmxfViewersVid.length; i++)
    {
      if (rmxfViewersVid[i] != null)
      {
        rmxfViewersVid[i].Destroy();       // ====================
        rmxfViewersVid[i] = null;
      }
    }
      if (rmxfViewerSnd != null)
        rmxfViewerSnd.Destroy();

      if (viewM != "Viewer")
      {
      // ----------------------- ����� ����� ActiveX ---------------------------
    var limit = ((useLimit != 0) && (useLimit != "no"));


if (url.length <  5) {is=2;} else
if (url.length < 10) {is=3;} else
if (url.length < 17) {is=4;} else
if (url.length < 26) {is=5;} else {is=6;};


    var table = document.getElementById('viewersTable');
    if (table)
      {
          while (table.rows.length > 0) table.deleteRow(table.rows.length - 1);
          var row = null;

          for (var i = 0; i < url.length; i++) // ���� �� ��������� ����������
          {
            if ((i % is) == 0) row = table.insertRow(-1); // ����� ������ ��� ������ "is" �����
            var cell = row.insertCell(-1);
    // ������ ������������
            cell.innerHTML = "<center><SPAN id='viewerPlace" + i + "'></SPAN>"                // ���� ���������
                           + "<br><font size=-5>(<a href=" + url[i] + ">" + url[i] + "</a>)</font></center>"; // ������� ��� ����������
          }
      }
      var w = width;
      var h = height;
//      var ratio = width/height; // �������� � -Pre-

      var VideoIndex = 0;
 
      if (refVideoStream[index])
    VideoIndex =refVideoStream[index];  

      if (url.length >= 2)
      {
        w = w / is; var h = h / is - 30; //ratio; document.body.offsetWidth
      }
 
      var Sizes = new Array();

     if ((typeof(rmxfSize)!='undefined') && (typeof(rmxfSize[index])!='undefined'))
      Sizes= rmxfSize[index].split(' ');

      
//      var addparams = recording==1?'EnableTitle='+document.getElementById('pathForRecord').value+'record': '';
      var addparams = recording==1?'EnableTitle=c:\\Vidicor_Record\\'+document.getElementById('pathForRecord').value+'\\':'';

      if (refParams[index].length > 0)
      addparams = refParams[index];
if (recording==1) alert(addparams);   
      for (var i = 0; i < url.length; i++)
      {
        var mult = (typeof(Sizes[i]) == 'undefined' ? 1 : Sizes[i]);
        rmxfViewersVid[i] = new RMXFViewer(url[i], document.getElementById('viewerPlace' + i),
      "Width=" + w*mult, "Height=" + h*mult,
      "bufferMin=" + bufferMin, "bufferMax=" + bufferMax,
      "EnableLimit=" + (limit ? "1" : "0"),
      "EnableAudio=" + enableAudio,
      "MulticastTimeout=" + (tryMulticast ? "3" : "-3"),
      "SelectedVideoStream="+VideoIndex,
//                  "StartPositionMs=-1",
//                  "maintainVideoAspectRatio=0",
      "RequestRetryTimeoutMS=2000",
      addparams);
//                  "PassiveMode=1", "PassivePort=9999",
//                  "ShowControls=0");

      }


      if (refLangUrl[currentLang] != "")
      {
  rmxfViewerSnd = new RMXFViewer(urlSnd, viewerPlaceSnd,
      "Width=" + width, "Height=0",
      "bufferMin=" + bufferMin, "bufferMax=" + bufferMax,
      "EnableLimit=" + (limit ? "1" : "0"),
      "EnableVideo=0",
      "MulticastTimeout=" + (tryMulticast ? "3" : "-3"));
      }

    if (refLang.length == 1) {var el = document.getElementById('audioCell'); if (el) el.height = 1};

      // ----------------------- / ����� ����� ActiveX ---------------------------
    }

    else

    {
      // ----------------- ����� ��������� ����������� Vidicor Watcher ------------------

        if (refLangUrl[currentLang] != "") {location.href = urlSnd};

        for (var i = 0; i < url.length; i++)
          {
            window.setTimeout("location.href=\'"+url[i]+"\';", 100);
          }
      // ---------------- / ����� ��������� ����������� Vidicor Watcher -----------------
    }


    var imgTag;
    if (document.getElementById('idCommentMailAddressA'))
    document.getElementById('idCommentMailAddressA').href = refMail[index];

    if (refPic[index] != "") { imgTag = "<img src=\'Images/" + refPic[index] + "\'>"; }

    else 
    { if (cnt == 1) { imgTag = "<img src=\'Images/play.gif\'> "; }
          else      { imgTag = "<img src=\'Images/" + (index+1) + ".gif'> "; }
    }

    if (refTxt[index].length != 0 && document.getElementById('viewerTitle')) 
     document.getElementById('viewerTitle').innerHTML = '<table  Title=\'�������� ������ �����/������� �������� ��� ����������\'><tr><td valign=middle>&nbsp;' + imgTag + '</td><td>&nbsp</td><td valign=middle><center><b>' + refTxt[index] + '</b><br><small><small><small><small>(������������: <b Title="������������ ���������� �� ���� �������������� (�����������-�����������). ����� -- ��� ����������">' + URLBaseName[URLBaseIndex] + '</b>)</small></small></small></small></center></td></tr></table>';

//  for (var i = 0; i < url.length; i++)
//    {
//    viewerTitle.innerHTML += '<br><font size=-5><b>� ' + (i+1) + ':</b> ('+ pre_config_file.src + '+' + user_config_file.src + '[<b>' + (i+1) + '</b>]: <a href='+url[i]+'>'+url[i]+'</a> �� '+ URLBaseName[URLBaseIndex] + ')</fonf>';
//    }
  }


  var playSound   = 1;
  var currentRef  = 1;
  var currentLang = 1;
  var recording = 0;

  function ChangeLanguage()
  {
    var el = document.getElementById('LanguageSelect');
    if (el) currentLang = el.selectedIndex;
    var URLBaseS = document.getElementById('URLBaseS');
    if (URLBaseS) playVideo(URLBaseS.value, currentRef, viewMethod);
  }



// ================================ ����� ������ ���������� ������� ===================================

    var serversTable = document.getElementById('serversTable');
if (serversTable)
{   
      var row  = serversTable.insertRow(-1);
      var cell = row.insertCell(-1);

  var txtHtml = "<center><table width=95% bgcolor=#eeeeee cellspacing=20 cellpadding=0 border=0><tr><td valign='middle'>";

     if ((typeof(refDiv)=="undefined")) var refDiv = new Array();
     if ((typeof(refDiv[0])!=undefined) && (refDiv[0] == "")) // ���� refDiv[0] �� ����� -- �������� �������������� �������
     {
      txtHtml += "<center>";
     }

  for (var i = 0; i < refUrl.length; i++) // ���� �� ����������� �������� � ������ �� ������ ������� �������
  {

     if ((refDiv[i]) != "") // ���� refDiv != "", �� �������� ��������� �������
     {
      txtHtml += refDiv[i];
     }

// if (i == currentRef) {txtHtml += ""};
  if (typeof(widthButton1)==undefined) 
  var widthButton1 = 60;
  if (typeof(widthButton2)==undefined)
        var widthButton2 = 100;
 

  var link = "onClick='var URLBaseS = document.getElementById(\"URLBaseS\");playVideo(URLBaseS.value, " + i + ", viewMethod);return false' href='#" + (i+1) + "' target='_self'";

    txtHtml += " <table  border=1 style='display:inline' cellspacing=0 cellpadding=0 bgcolor=#cdcdcd><tr><td align=center valign=middle>"; // "�������" "������" ������
    txtHtml += " <table  border=0 height=45 cellspacing=1 cellpadding=1><tr>"; // "����������" "������" ������
    txtHtml += "<td  width=" + widthButton1 + " align=center valign=middle><center><a class='channelbutton' " +  link + " title='" + refTxt[i] + "'>";

    txtHtml += "<img src='Images/";

    if (refPic[i] != "") { txtHtml += refPic[i]; }
    else 
      { if (refUrl.length == 1) { txtHtml += "play.gif"; }
        else                    { txtHtml += (i+1) + ".gif"; }
      }

    heightImg = 30;
    txtHtml += "' height='" + heightImg;
    txtHtml += "' border=0></a></center></td>";

    txtHtml += "<td width=" + widthButton2 + " align=center valign=middle>&nbsp;<small><small><a " + link + " title='" + refUrl[i] + "'>"+ refTxt[i] + "</a></small></small>&nbsp;</td>";

    txtHtml += "</tr></table>";
    txtHtml += "</td></tr></table>&nbsp;. ";
  }

    txtHtml += "</td></tr></table></center>";
    cell.innerHTML = txtHtml; //  <<<------------------------------------------------
}
  
// ================================ / ����� ������ ���������� ������� ===================================


// ���������� ��������� ������ � �����
  var el = 0;
  el = document.getElementById('recvProtocol');
  if (el) 
  el.value = protocol;

  el = document.getElementById('recvPort');
  if (el) el.value = port;
  el = document.getElementById('recvbufferMid');
  if (el) el.value = bufferMid;

  // �������� �������� �� ��������� ��� �� ��������� ������
  //alert(refUrl);
  //debugger;

  if (typeof(cnt)!='undefined' && cnt > 1) { currentRef = defaultRef-1;}
  else         { currentRef = 0; }
  var pos=document.URL.indexOf('#')+1;
  if (pos != 0) {currentRef = document.URL.substring(pos) };
  

   var ref_index = -1;   
//   alert(typeof(currentRef));
   for (var i=0; i<refUrl.length; i++)  
   if ( typeof(currentRef) == 'string')
   if ( refUrl[i].toLowerCase()  == currentRef.toLowerCase() )
  {
   ref_index = i; break;
  };

   if (ref_index < 0)
   {
     if (isNaN(currentRef))  currentRef = 0;
//      else currentRef-=1;
     if (currentRef > refUrl.length) currentRef = defaultRef-1;
   }
    else
    currentRef = ref_index;

  if (refLang.length > 1)
   currentLang = defaultLang - 1;
  else 
   currentLang = 0;

  var el = document.getElementById('URLBaseS');
  if (el)
  playVideo(el.value, currentRef, viewMethod);

// body.bgColor         = body_bgColor;
el = document.getElementById('allWindow');
if (el) el.bgColor    = allWindow_bgColor;
el = document.getElementById('videoCell');
if (el) el.bgColor    = allWindow_bgColor; // address
el = document.getElementById('audioCell');
if (el) el.bgColor    = allWindow_bgColor; // alt sound
el = document.getElementById('controlCell1');
if (el) el.bgColor = controlBar_bgColor; // reflector
el = document.getElementById('controlCell2');
if (el) el.bgColor = controlBar_bgColor; // channek title
el = document.getElementById('controlCellB');
if (el) el.bgColor = controlBar_bgColor; // protocol, port, buffer