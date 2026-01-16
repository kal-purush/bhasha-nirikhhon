var inputValues=document.getElementById('listContent');
function addItems(){
    if(inputValues.value!=""){
    var elementCreater = document.createElement('li');
    var elementCreaterVaue=document.createTextNode(inputValues.value);
    elementCreater.appendChild(elementCreaterVaue)
    var valerAdderInUL= document.getElementById('listItems');
     valerAdderInUL.append(elementCreater)
     inputValues.value=""

     var buttonTagCreater=document.createElement('button')
     var buttonTagText=document.createTextNode("Edit")
     buttonTagCreater.setAttribute('class' ,'btn')
     buttonTagCreater.setAttribute('onclick' ,'SingleEdit(this)')
     buttonTagCreater.appendChild(buttonTagText)

     

     var sectionForI=document.createElement('button')
     sectionForI.setAttribute('class', 'btn delteIcon')
     sectionForI.setAttribute('onclick', 'DeletSelctedItem(this)')
     var aweSomeIconElemnt=document.createElement('i')
     aweSomeIconElemnt.setAttribute("class","fa fa-trash-o")
     sectionForI.appendChild(aweSomeIconElemnt)

     var divGenratorFOrEditTast=document.createElement('span')
     divGenratorFOrEditTast.setAttribute("class" ,'editList');
     divGenratorFOrEditTast.appendChild(buttonTagCreater)
     divGenratorFOrEditTast.appendChild(sectionForI)
     
     elementCreater.appendChild(divGenratorFOrEditTast);
    var hrLine=document.createElement('hr')
    valerAdderInUL.appendChild(hrLine);
    console.log(elementCreater)
    }
    

     
}
function DeleteAll(){
  var deleteAl=document.getElementById('listItems');
  deleteAl.innerHTML=""   
}
function DeletSelctedItem(e){
   
 e.parentNode.parentNode.remove()
    
}

function SingleEdit(e){
  console.log('hyfrgj')
  var enter=prompt("Enter your Text",e.parentNode.parentNode.firstChild.nodeValue)
  e.parentNode.parentNode.firstChild.nodeValue=enter
}