
    let list_div
    let form
    let input

    function onLoad(){
        list_div = document.getElementById("form_list")
        form = document.getElementById("addToListForm")
        input = form.querySelector(":scope > input")
    }
    document.addEventListener("DOMContentLoaded", onLoad)

    function addToList(){
        const list = Array.from(list_div.querySelectorAll(":scope > ul > li")).map(x => x.innerText)
        list.push(input.value)
        input.value = ""
        list_div.innerHTML = formatAsList(list)
    }

    function removeFirstOfList(){
        const list = Array.from(list_div.querySelectorAll(":scope > ul > li")).map(x => x.innerText)
        list.shift()
        list_div.innerHTML = formatAsList(list)
    }

    function removeLastOfList(){
        const list = Array.from(list_div.querySelectorAll(":scope > ul > li")).map(x => x.innerText)
        list.pop()
        list_div.innerHTML = formatAsList(list)
    }

    function formatAsList(list){
        if(list.length == 0){
            return ""
        } else {
            return "<ul><li>" + list.join("</li><li>") + "</li></ul>"
        }
    }