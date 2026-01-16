interface AjaxRequestParam {
  url : string;
  type : string;// 'GET' or 'POST'. if undefined then 'GET'.
  contentType: string;// if undefinded then 'application/x-www-form-urlencoded'
  data: string;
  dataType:string;// response type. if 'json' then response parsed to json.
  
  success : (res: any, statuscode: number) => void;// if dataType is 'json' then res is Object(json). Otherwise res is string.
  error: (statuscode: number) => void;
}
function nanoAjax(p: AjaxRequestParam){
  var req: XMLHttpRequest = null;
  if (window['ActiveXObject']) req = new ActiveXObject('Microsoft.XMLHTTP');
  else if (window['XMLHttpRequest']) req = new XMLHttpRequest();
  if(!req) return;
  
  req.onreadystatechange = () => {
    if(req.readyState < 4) return;// not completed
    
    if(req.status < 400) {// success
      if(p.success) {
        p.success(p.dataType == 'json' ? JSON.parse(req.responseText) : req.responseText, req.status);
      }
    } else if(p.error) {// failed
      p.error(req.status);
    }
  };

  if (p.type == 'POST') {
    req.open("POST", p.url, true);
    req.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
    req.setRequestHeader('Content-type', p.contentType || 'application/x-www-form-urlencoded');
  } else {
    req.open("GET", p.url, true);
  }

  req.send(p.data || '');
}