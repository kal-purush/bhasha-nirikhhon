//calender creation

var days = ["Sun ", "Mon ", "Tue ", "Wed ", "Thu ", "Fri ", "Sat "];
var months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
var date = new Date();
console.log(date);//Thu Aug 26 2021 21:17:48 GMT+0530 (India Standard Time)
var year = date.getFullYear();
var month = date.getMonth();
console.log(month + " " + year);
var htmlCalender = document.querySelector(".calender");
// var monthYear = document.querySelector(".update");
var forfunyear;
function createcalender(year, month) {
    var monthYear = document.createElement("h1");
    var componentmonth = document.createTextNode(months[month]);
    var componentyear = document.createTextNode(year);
    monthYear.append(componentmonth);
    monthYear.append(document.createTextNode (" "));
    monthYear.append(componentyear);
    forfunyear = year + "-" + month;

    htmlCalender.append(monthYear);
    var firstDay = new Date(year, month, 01).getDay();
    var dayHas = 32 - (new Date(year, month, 32).getDate());
    var table = document.createElement("table");
    var tablehead = document.createElement("thead");
    var tablerow = document.createElement("tr");
    for (var d = 0; d < days.length; d++) {
        var th = document.createElement("th");
        var content = document.createTextNode(days[d]);
        th.append(content);
        tablerow.append(th);
    }
    tablehead.append(tablerow);
    table.append(tablehead);

    var tablebody = document.createElement("tbody");
    var currentdate = 1;
    for (var week = 0; week < 5; week++) {
        var row = document.createElement("tr");
        for (var wday = 0; wday <= 6; wday++) {
            var body = document.createElement("td");
            if (currentdate > dayHas) {

            }
            else if (week == 0 && wday < firstDay) {

            }
            else {
                var contentbody = document.createTextNode(currentdate);
                body.append(contentbody);

                body.setAttribute("id", forfunyear + "-" + currentdate);
                currentdate++;
            }
            row.append(body);
        }
        tablebody.append(row);
    }

    table.append(tablebody);
    htmlCalender.append(table);

    function show(fuly2) {
        firebase.database().ref('Year/' + fuly2).on('value', function (snapshot) {
            if (snapshot.exists()) {

                var t = fuly2.split("-")[2];
                var tb = document.getElementById(fuly2);
                
                tb.lastChild.innerHTML =" ";
                var div = document.createElement('div');
                div.id = 'container';
                snapshot.forEach(function (ChildSnapshot) {
                    var data = ChildSnapshot.val().Description;
                    var tit = ChildSnapshot.val().Title;
                    var d = tit;
                    var div1 = document.createElement("div");
                    div1.id = "childdiv"
                    div1.append(d);
                    div.append(div1);
                    tb.append(div);
                });
            }
        });
    }
    diss(show);
    function diss(callback) {
        for (var k = 0; k <= 31; k++) {
            var fuly2 = forfunyear + "-" + k;
            callback(fuly2);
        }

    }
}

createcalender(year, month);


function getplus() {


    month++;
    if (month == 12) {
        year++;
        month = 0;
    }
    htmlCalender.innerHTML = "";

    createcalender(year, month);
}


function getminus() {
    month--;
    if (month < 0) {
        year--;
        month = 11;
    }
    htmlCalender.innerHTML = "";

    createcalender(year, month);
}






var viewdatacontent = document.querySelector("#view-data-content");


function show3(fuly2) {
    firebase.database().ref('Year/' + fuly2).on('value', function (snapshot) {
        viewdatacontent.innerHTML= " ";
        if (snapshot.exists()) {
            
            var t = fuly2.split("-")[2];
            var div = document.createElement('div');
            div.id = 'container3';
            div.className = fuly2;
            snapshot.forEach(function (ChildSnapshot) {
                var data = ChildSnapshot.val().Description;
                var tit = ChildSnapshot.val().Title;
                var fro = ChildSnapshot.val().From;
                var to = ChildSnapshot.val().To;
                var d = `Title:${tit}, Description:${data}, From:${fro}, To:${to}`;

                var div1 = document.createElement("div");
                div1.className = "childdiv3";
                div1.id = `${ChildSnapshot.key}`;
                div1.append(d);
                div.append(div1);
                viewdatacontent.append(div);
            });
        }
    });
}
var membercontent=document.querySelector("#member-content");
function show4(dAte){
    firebase.database().ref('Email/' +dAte).on('value',function(snapshot){
        membercontent.innerHTML=" ";
        if(snapshot.exists()) {
            
            var div = document.createElement('div');
            div.id = 'container6';
            div.className = dAte;
            snapshot.forEach(function (ChildSnapshot) {
                var Name = ChildSnapshot.val().Name;
                var Email = ChildSnapshot.val().Email;
                var d = `Name:${Name}, Email:${Email}`;

                var div1 = document.createElement("div");
                div1.className = "childdiv6";
                div1.id = `${ChildSnapshot.key}`;
                div1.append(d);
                div.append(div1);
                membercontent.append(div);
            });

        }
    });

}








// notepad from validations
function submitform(data, e) {

    e.preventDefault();

    var formdata = {};
    for (var i = 0; i < data.length - 1; i++) {

        formdata[data[i].name] = data[i].value;
    }

    console.log(formdata);
    var Array = formdata.date.split('-');
    var fireYear = Array[0];//2021
    var fireMonth = Array[1];//07
    firebase.database().ref('Year/' + formdata.date).push({
        Title: formdata.title,
        From: formdata.from,
        To: formdata.to,
        Description: formdata.description
    });

    e.submitform;
    var clear1 = document.querySelector(".clear1");
    setTimeout(function () {
        clear1.click();
        modal.style.display = "none";
    }, 1000);

}


function addnew(data2, e) {
    e.preventDefault();
    var addnew = {};
    for (var i = 0; i < data2.length - 1; i++) {

        addnew[data2[i].name] = data2[i].value;
    }
    let email = addnew.newattendee;
    if (validateEmail(email)) {
        var frm = document.querySelector("#frm-id");
        firebase.database().ref('Email/' + frm['date'].value).push({
            Email: addnew.newattendee,
            Name:addnew.name
        });
    }
    else {
        alert("please select corrct mail");
    }
    e.submitform;
    var clear2 = document.querySelector(".clear2");
    setTimeout(function () {
        clear2.click();
        modal.style.display = "none";
    }, 1000);


}


function validateEmail(email) {
    const re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(email);
}

var updatefrm = document.querySelector("#update-frm");
var updatebtn = document.querySelector("#updatebtn");
function fillup(updateid) {

    updatefrm.elements['date'].value = updateid.path[1].className;
    firebase.database().ref('Year/' + `${updateid.path[1].className}/` + updateid.target.id).on('value', function (snapshot) {
        updatefrm.elements['title'].value = snapshot.val().Title;
        updatefrm.elements['from'].value = snapshot.val().From;
        updatefrm.elements['to'].value = snapshot.val().To;
        updatefrm.elements['description'].value = snapshot.val().Description;
    });
    
    deletebtn.addEventListener('click', function () {
        show3(updateid.path[1].className);
        firebase.database().ref('Year/' + `${updateid.path[1].className}/` + updateid.target.id).remove();
        updatedata.style.display = "none";
    });
    
    show3(updateid.path[1].className);

}
function close(updateid){
    updatebtn.addEventListener('click', function () {
        show3(updateid.path[1].className);
            // document.getElementById(`${updateid.path[1].className}`).lastChild.innerHTML = " ";
            console.log(updateid.target.id);
            firebase.database().ref('Year/' + `${updateid.path[1].className}/` + `${updateid.target.id}`).update({
                Title: updatefrm.elements['title'].value,
                From: updatefrm.elements['from'].value,
                To: updatefrm.elements['to'].value,
                Description: updatefrm.elements['description'].value
            });
            updatedata.style.display = "none";
            show3(updateid.path[1].className);
        });
        
}


membercontent.addEventListener("click", function(e) {
             console.log(e);
             firebase.database().ref('Email/'+`${e.path[1].className}/`+e.path[0].id).remove();
             show4(e.path[1].className);
             member.style.display="none";
});







// popup showing 
var modal = document.getElementById("myModal");
var span = document.getElementsByClassName("close")[0];
var viewdata = document.getElementById("view-data");

var forfunctiondate;
htmlCalender.addEventListener("click", function (e) {

    forfunctiondate = e.target.id;
    if (e.path[0].id == "childdiv") {
        var checkdiv = e.path[2].id;

        viewdata.style.display = "block";


        span.onclick = function () {
            viewdata.style.display = "none";

        }

        window.onclick = function (event) {
            if (event.target == viewdata) {
                viewdata.style.display = "none";
            }
        }

        show3(checkdiv);
    }
    else {
        modal.style.display = "block";
        var frmid = document.querySelector('input[name="date"]');
        frmid.value = e.target.id;


        span.onclick = function () {
            modal.style.display = "none";
        }

        window.onclick = function (event) {

            if (event.target == modal) {
                modal.style.display = "none";
            }
        }
    }




});

var updatedata = document.querySelector("#update-data");
viewdatacontent.addEventListener("click", function (e) {


    updatedata.style.display = "block";


    span.onclick = function () {
        updatedata.style.display = "none";
    }

    window.onclick = function (event) {
        if (event.target == updatedata) {
            updatedata.style.display = "none";
        }
        if (event.target.id == "view-data") {
            viewdata.style.display = "none";
        }
    }
    fillup(e);
    close(e);
});
var member=document.querySelector("#member");

function viewmembers(e)
{
    e.preventDefault;
    member.style.display="block";


    span.onclick = function () {
        member.style.display = "none";
    }

    window.onclick = function (event) {
         
        if (event.target == member) {
            member.style.display = "none";
        }
        if(event.target==modal) {
            modal.style.display="none";
        }
    }

    var frm=document.querySelector("#frm-id");
    show4(frm.elements["date"].value);
   

}


var nowdate=date.getFullYear()+"-"+date.getMonth()+"-"+date.getDate();
document.getElementById(nowdate).style.backgroundColor="#FFEDDA";