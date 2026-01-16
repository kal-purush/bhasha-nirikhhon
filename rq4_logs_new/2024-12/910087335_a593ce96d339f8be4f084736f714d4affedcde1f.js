const input_text=document.getElementsByClassName("input-text")[0];
const input_btn=document.getElementsByClassName("input-btn")[0];
const box=document.getElementsByClassName("history-box")[0];
const arr=[];

function match(){
    const val= input_text.value;
    console.log(val);
    if(!arr.includes(val)){
        arr.push(val);
        const newDiv = document.createElement('div');
        const textNode = document.createTextNode(val);

        const image1 = document.createElement("img");
        image1.src = "images/complete.png";
        image1.style.width='30px';
        image1.style.marginRight='10px';

        const del_img=document.createElement("img");
        del_img.src="images/delet.jpg";
        del_img.style.width='30px';


        const child_div = document.createElement('div');
        child_div.appendChild(image1);
        child_div.appendChild(del_img);
        newDiv.appendChild(textNode);
        newDiv.appendChild(child_div);
        box.appendChild(newDiv);
        newDiv.setAttribute("class","d-flex gap-5 justify-content-between align-items-center mt-4");
        newDiv.style.border="1px solid black";
        newDiv.style.height="36px";
        input_text.value="";

        image1.addEventListener('click', complete);
        function complete(textNode){
            alert("hi");
            // textNode.setAttribute("class","text-decoration-line-through");
            // const textNode1 = document.createTextNode(val);
            // val.setAttribute("class","text-decoration-line-through");
        }
        
    }
    else{
        console.log("list is already found");
    }
}


input_btn.addEventListener("click", match);