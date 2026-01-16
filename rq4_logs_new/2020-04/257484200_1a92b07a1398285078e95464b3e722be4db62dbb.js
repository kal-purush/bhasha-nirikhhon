function fnReg()
{
  var  idRef=document.getElementById('txtId');
  var  nameRef=document.getElementById('txtName');
   var  emailRef=document.getElementById('txtEmail');
   var  phoneRef=document.getElementById('txtPhone');
   var  acnoRef=document.getElementById('txtAcno');
   var pwdRef=document.getElementById('txtPwd');

   var  id=idRef.value;
   var  name=nameRef.value;
    var email=emailRef.value;
    var phone=phoneRef.value;
    var acno=acnoRef.value;
    var  pwd=pwdRef.value

    dataObj={
        "id":id,
        "name":name,
        "email":email,
        "phone":phone,
        "acno":acno,
        "pwd":pwd,
    }

var httpObj = new XMLHttpRequest();
    httpObj.open('post','http://localhost:2020/users/reg-cus');
    httpObj.setRequestHeader('Content-Type','application/json');
    httpObj.send(JSON.stringify(dataObj));
    httpObj.onload =function(){
       var res = httpObj.responseText;
       res=JSON.parse(res);
       if(res.affectedRows>0)
       {
           fnGetcustomers();
           alert('inserted successfully');
           idRef.value='';
           nameRef.value=' ';
           emailRef.value='';
           phoneRef.value=' ';
           acnoRef.value=' ';
           pwdRef.value=' ';
       }
    }
    httpObj.onerror = function(){
        debugger;
        console.log(httpObj.responseText);
    }
}


function fnGetcustomers() {
    var httpObj = new XMLHttpRequest();
    httpObj.open('get', 'http://localhost:2020/users/get-cust');
    httpObj.send();
    httpObj.onload = function() {
        var res = httpObj.responseText;
        res = JSON.parse(res);
        customer = res;
        prepareTable(res);
        
    }
    httpObj.onerror = function() {
        debugger;
    }
}


function fnBodyLoad() {
    fnGetcustomers();

    regBtnRef = document.getElementById('reg');
     idRef=document.getElementById('txtId');
     nameRef=document.getElementById('txtName');
       emailRef=document.getElementById('txtEmail');
      phoneRef=document.getElementById('txtPhone');
        acnoRef=document.getElementById('txtAcno');
       pwdRef=document.getElementById('txtPwd');
   
      updateBtnRef = document.getElementById('update');
      updateBtnRef.style.display = 'none';
}

function prepareTable(data) {
    var trs = '';
    data.forEach(function (o, i) {
        debugger;
        trs = trs + "<tr><td>" + o.id+ "</td><td>" + o.name + "</td><td>" + o.email + "</td><td>"+o.phone+"</td><td>"+o.acno+"</td></td>"+"</td><td>"+o.pwd+"</td><td><input type=button value=edit onclick=fnEdit(" + o.id + ") /></td><td><input type=button value=delete onclick=fnDelete("+o.id+") /></td></tr>";
    })
    var tbl = "<table border=1px><tr><th>id</th><th>name</th><th>email</th><th>phone</th><th>acno</th><th>pwd</th><th>Edit</th><th>Delete</th></tr>" + trs + "</table>";
    document.getElementById('tbl-data').innerHTML = tbl;
}
function fnEdit(id) {
    regBtnRef.style.display = 'none';
    updateBtnRef.style.display = 'block';
    idRef.setAttribute('disabled','disabled');
    var custObj = customer.find(function (o) {
        return o.id == id;
    })
    idRef.value=custObj.id;
    nameRef.value=custObj.name;
    emailRef.value=custObj.email;
   phoneRef.value=custObj.phone;
   acnoRef.value=custObj.acno;
   pwdRef.value=custObj.pwd;
}

function fnDelete(id)
{
    var isOk=confirm('r u sure');
    if(isOk)
    {
        var httpObj=new  XMLHttpRequest();
        httpObj.open('get','http://localhost:2020/users/del-cust?id='+id);
        httpObj.send();
        httpObj.onload =function(){
           var res=httpObj.responseText;
           res=JSON.parse(res);
          if(res.affectedRows>0){
              alert('deleted successfully');
              fnGetcustomers();
          }
        }
       
    }
    }
    function fnUpdate()
    {
        
        var id=idRef.value;
      var  name=nameRef.value;
      var  email=emailRef.value;
      var  phone=phoneRef.value;
      var  acno=acnoRef.value;
      var pwd=pwdRef.value

        dataObj={
            "id":id,
            "name":name,
            "email":email,
            "phone":phone,
            "acno":acno,
            "pwd":pwd
        }
        var httpObj = new XMLHttpRequest();
    httpObj.open('post','http://localhost:2020/users/update-cust?id='+id);
    httpObj.setRequestHeader('Content-Type','application/json');
    httpObj.send(JSON.stringify(dataObj));
    httpObj.onload =function(){
       var res = httpObj.responseText;
       res=JSON.parse(res);
       if(res.affectedRows>0)
       {
         
           alert('updated successfully');
           idRef.value='';
            nameRef.value=' ';
            emailRef.value='';
            phoneRef.value=' ';
            acnoRef.value=' ';
            pwdRef.value=' ';
            idRef.removeAttribute('disabled');
            fnGetcustomers();
       }
    }
    httpObj.onerror = function(){
        debugger;
        
    }
}
    