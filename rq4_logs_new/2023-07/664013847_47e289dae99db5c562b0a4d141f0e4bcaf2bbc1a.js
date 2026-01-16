const feelingsDiv = document.getElementById('feelings-div');
const addFeelingsToEntryButton = document.getElementById("addFeelingsToEntryButton");


addFeelingsToEntryButton.addEventListener('click', getSelectedFeelings)


let feelingsList = ['happiness','love','relief','joy','pride','worry','stress','anxious', 'lonely', 'hopeless', 'lost','worried','annoyed','peace','excitement'];



function createFeelingsButtons(){
    let htmlToAdd = ``
    for(i=0 ; i < feelingsList.length ; i++){
        
            
            htmlToAdd +=`
            
            <button onclick="changeFeelingColor(${i})" id="feelingNo-${i}" class="feelingButtonRemove btn bg-ash text-light m-1"> ${feelingsList[i]} </button>
            `     
      
    }

   
    return htmlToAdd
}

function renderFeelingsDiv(){
    feelingsDiv.innerHTML = createFeelingsButtons();

}

renderFeelingsDiv()


function getSelectedFeelings(){
    let feelings = []
    for(i=0 ;i < feelingsList.length ; i++){
        let feelingButton = document.getElementById(`feelingNo-${i}`);
        if(feelingButton.classList.contains('feelingButtonAdd')){
            feelings.push(feelingsList[i])
        }
    }
    console.log(feelings);
}



function changeFeelingColor(no){
    let feelingButton = document.getElementById(`feelingNo-${no}`);

    if(feelingButton.classList.contains('feelingButtonAdd')){

        feelingButton.classList.remove('feelingButtonAdd');
        feelingButton.classList.add('feelingButtonRemove');

    } else if(feelingButton.classList.contains('feelingButtonRemove')){

        feelingButton.classList.remove('feelingButtonRemove');
        feelingButton.classList.add('feelingButtonAdd');

    }
}



