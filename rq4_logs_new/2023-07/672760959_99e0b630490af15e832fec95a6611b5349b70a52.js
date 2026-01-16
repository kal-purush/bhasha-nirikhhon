//유저가 값을 입력한다
//플러스 버튼 클릭하면 할 일이 추가 된다
//유저가 delete 버튼 누르면 할 일 삭제
//체크 버튼 누르면 할 일이 끝나면서 밑줄이 간다. 
//진행 중 끝남 탭을 누르면 언더바가 이동한다
//끝난 탭은 끝난 곳에, 진행 중인 것은 진행 중인 곳으로 이동
//전체 탭 누르면 다시 전체 아이템으로 돌아옴

let taskInput = document.getElementById("task-input");
let addButton = document.getElementById("add-button");
let tabs = document.querySelectorAll(".task-tabs div"); 

// task-tabs 아래에 있는 div를 모두 가져올 때 
let taskList = [];
let mode = "all"; 
// mode를 ""; 로 처음에 해주고 나중에는 all로 바꿔줌
// 처음에 all이어야 ui가 제대로 뜬다.
let filterList=[];
// 다른 함수에서도 접근할 수 있도록 filterList 전역 변수로 만들어줌 

let underLine = document.getElementById("under-line");
let underlineMenus = document.querySelectorAll("section div div");
console.log(underLine)
console.log(underlineMenus)

underlineMenus.forEach(menu => menu.addEventListener("click",(e)=>underlineIndicator(e)));

function underlineIndicator(e) {
    underLine.style.left = e.currentTarget.offsetLeft + "px";
    underLine.style.width = e.currentTarget.offsetWidth + "px";
    underLine.style.top = 
        e.currentTarget.offsetTop + e.currentTarget.offsetHeight + "px";
}
    


for (let i=1; i<tabs.length; i++){
    tabs[i].addEventListener("click",function(event){
        filter(event);
    });
}
// 어떤 탭을 클릭했는지 알아야 하기 때문에 이벤트를 해서 filter에 event 넘겨 줌


// 유저가 +버튼을 클릭하면 할 일이 추가된다
addButton.addEventListener("click",addTask)

function addTask(){
    let task = {
        id: randomIDGenerate(),
        taskContent : taskInput.value,
        isComplete : false,
    };
    taskList.push(task);
    console.log(taskList);
    render();
}

// 
function render() { 
    let list = [];
        if (mode == "all"){
            list = taskList;
        }else if (mode == "ongoing" || mode == "done"){
            list = filterList;
        }
        
        // render() 함수 안에 의미 없는 list = [];가 있음
        // render()는 ui를 보여주는 함순데 taskList 보여줄 지 filterList 보여줄지 선택함 
        // render = filterList 에 따라 화면에서 all일 때 ongoing 보여주는 결과 해결하기 위해 
        // 아레에 있는 taskList도 전부 list로 프린트해주기 (ctrl+D로 한 번에) 백틱 부분만 왜?
    let resultHTML = "";
    for (let i = 0; i < list.length; i++) {
        if (list[i].isComplete == true){
            resultHTML += `<div class="task">
                    <span class="task-done grey">${list[i].taskContent}</span>
                        <div class="button-box"> 
                        <button class="check-button" onclick="toggleComplete('${list[i].id}')"><i class="fa-solid fa-rotate" style="color: #27be70;"></i></button>
                        <button class="delete-button" onclick="deleteTask('${list[i].id}')"><i class="fa-solid fa-xmark" style="color: #ff0000;"></i></button>
                    </div>
                </div>`;
        }else {
            resultHTML += `<div class="task">
                <span>${list[i].taskContent}</span>
                    <div class="button-box"> 
                        <button class="check-button" onclick="toggleComplete('${list[i].id}')"><i class="fa-solid fa-check" style="color: #44cd32;"></i></button>
                        <button class="delete-button" onclick="deleteTask('${list[i].id}')"><i class="fa-solid fa-xmark" style="color: #ff0000;"></i></button>
                    </div>
                </div>`;
            }
        }

       

    document.getElementById("task-board").innerHTML = resultHTML;
    }

    function toggleComplete(id) {
        for (let i=0; i<taskList.length; i++) {
            if (taskList[i].id == id) {
                taskList[i].isComplete = !taskList[i].isComplete;
                break;
            }
        }
        render();
    }

    function deleteTask(id) {
        for (let i = 0; i <taskList.length; i++) {
            if (taskList[i].id == id) {
                taskList.splice(i, 1)
                break;
            }
        }
        render();
    }
    
    function filter(event) {
            mode = event.target.id;
            filterList = [];

            if (mode == "all") {
            render();
            // render()는 전체를 보여주는 함수 
            } else if (mode == "ongoing"){
                for (let i=0; i<taskList.length; i++) {
                 if (taskList[i].isComplete == false) {
                    filterList.push(taskList[i])
                }
            }
            render();
            } else if (mode == "done"){
                 for (let i=0; i<taskList.length; i++){
                  if (taskList[i].isComplete == true){
                    filterList.push(taskList[i])
                }
            }
            render();
        }
        // render()는 taskList를 출력하므로, render()를 filterList로 바꿔주자 
        console.log(filterList)
      
    }
    // 클릭 했을 때 발생되는 모든 상황에 대해 알려주는 것이 이벤트 (어떤 아이템을 클릭했는지, 이벤트가 어떤 타입인지 등등)
    // event.target하면 그중에서 어떤 걸 클릭했는지 알게 해줌 

    function randomIDGenerate(){
        return "_" + Math.random().toString(36).substring(2, 9);
    }

   