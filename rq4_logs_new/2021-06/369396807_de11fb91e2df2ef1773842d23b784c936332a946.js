const form=document.getElementById("menu_form");

form.addEventListener("submit",addmenu)

function addmenu(e)
{
    e.preventDefault();

    const xhr=new XMLHttpRequest();
    const menuname=document.getElementById("i_name").value;
    const menuprice=document.getElementById("i_price").value;
    xhr.onreadystatechange=function (){
        if(this.readyState==4&&this.status==200)
        {
            if(this.responseText==="")
            {
                alert("menu not added");
            }
            else {
                alert("Item Added");

            }
        }
    }
    xhr.open("POST","http://localhost:8080/menu/create",true);
    const data={
        "iteamName":menuname,
        "price":menuprice,
        "restraunt":sessionStorage.getItem("rid")
    }
    xhr.send(JSON.stringify(data));
}
function findrest()
{

    const xhr=new XMLHttpRequest();

    xhr.onreadystatechange=function (){
        if(this.readyState==4&&this.status==200)
        {
            const list=JSON.parse(this.responseText);

            (adding(list));

        }
    }
    const id=sessionStorage.getItem("rid")
    xhr.open("GET",`http://localhost:8080/rest/${id}`,true);
    xhr.send();



}
function adding(data)
{
    document.getElementById("r_name").innerHTML=data.name;
    document.getElementById("r_tl").innerHTML=data.tagline;
    document.getElementById("r_add").innerHTML=data.address;


    const xhr=new XMLHttpRequest();

    xhr.onreadystatechange=function (){
        if(this.readyState==4&&this.status==200)
        {const list=JSON.parse(this.responseText);

        }
    }
    const id=data.id;
    xhr.open("GET",`http://localhost:8080/menu/get/${id}`,true);
    xhr.send();
}