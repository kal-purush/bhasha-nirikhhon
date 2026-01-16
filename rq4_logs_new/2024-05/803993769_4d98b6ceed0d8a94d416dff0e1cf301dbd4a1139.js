let searchBox = document.getElementById('search-input');
let creationDateS = document.getElementById('creationdatestart');
let creationDateE = document.getElementById('creationdateend');
let valueStart = document.getElementById('valueStart');
let valueEnd = document.getElementById('valueEnd');

let firstAlbumS = document.getElementById('firstalbumstart');
let firstAlbumE = document.getElementById('firstalbumend');
let valueAlbumStart = document.getElementById('valueAlbumStart');
let valueAlbumEnd = document.getElementById('valueAlbumEnd');

let checkAllMembersBox = document.getElementById('allmembers');
let checkMembersBox = document.querySelectorAll("[type=checkbox][name=members]");


let keywords, creationDateStart, creationDateEnd, firstAlbumStart, firstAlbumEnd;

keywords = " ";
creationDateStart = creationDateS.value;
creationDateEnd = creationDateE.value;

firstAlbumStart = firstAlbumS.value;
firstAlbumEnd = firstAlbumE.value;

searchBox.addEventListener('keyup', (e) => {
    keywords = e.target.value.toLowerCase().trim();
    index = keywords.indexOf("-");
    if (index > -1) {
        keywords = keywords.substring(0, index).trim();
    }
    search()
})

creationDateS.addEventListener('input', (e) => {
    creationDateStart = e.target.value;
    if (creationDateStart > creationDateEnd) {
        creationDateE.value = creationDateStart
        valueEnd.innerText = creationDateStart
    }
    search()
})

creationDateE.addEventListener('input', (e) => {
    creationDateEnd = e.target.value;
    if (creationDateEnd < creationDateStart ) {
        creationDateS.value = creationDateEnd
        valueStart.innerText = creationDateEnd
    }
    search()
})

firstAlbumS.addEventListener('input', (e) => {
    firstAlbumStart = e.target.value;
    if (firstAlbumStart > firstAlbumEnd) {
        firstAlbumE.value = firstAlbumStart
        valueAlbumEnd.innerText = firstAlbumStart
    }
    search()
})

firstAlbumE.addEventListener('input', (e) => {
    firstAlbumEnd = e.target.value;
    if (firstAlbumEnd < firstAlbumStart ) {
        firstAlbumS.value = firstAlbumEnd
        valueAlbumStart.innerText = firstAlbumEnd
    }
    search()
})

checkAllMembersBox.addEventListener('click', checkAllMembers)
function checkAllMembers() {
    const membersNum = document.getElementsByName("members");
    for(let i = 0; i< membersNum.length; i++){  
        if (membersNum[i].type == 'checkbox')  
            membersNum[i].checked = checkAllMembersBox.checked;  
        }  
    search()
}

checkMembersBox.forEach(elm => elm.addEventListener('click',search))

let timeoutId;

function search() {

    clearTimeout(timeoutId);

    let artistName = document.getElementById('search-artist');
    let artistList = document.getElementsByClassName('opt-artist');
    if (artistList) {
        Array.from(artistList).forEach(elm => elm.remove())
    }
    timeoutId = setTimeout(() => {
      
        let elements = document.getElementsByClassName('main-article');
        let name, authorMemb, creationDate, firstAlbumDate;

        let checkedMembersNode = document.querySelectorAll('[name=members]:checked');
        let checkedMembers = [];
        Array.from(checkedMembersNode).forEach((elm) => checkedMembers.push(parseInt(elm.value)));
        // console.log(checkedMembersNode[1].value)
        // for (i=0; i<checkedMembersNode.length; i++) {
        //     console.log(checkedMembersNode.item(i).value)


        Array.from(elements).forEach((elm) => {
            nameGroup = elm.getAttribute('data-name');
            allNames = elm.getAttribute('data-allNames').toLowerCase();
            authorMemb = elm.getAttribute('data-authorMemb');
            creationDate = parseInt(elm.getAttribute('data-creationDate'));
            membersCount = parseInt(elm.getAttribute('data-membersCount'));
            // alert("hello " + creationDate + typeof creationDate);
            firstAlbumDate = parseInt(elm.getAttribute('data-firstAlbumDate').substring(elm.getAttribute('data-firstAlbumDate').length-4));



            if ((creationDate >= creationDateStart && creationDate <= creationDateEnd)
                && (firstAlbumDate >= firstAlbumStart && firstAlbumDate <= firstAlbumEnd)
                && allNames.includes(keywords)
                && checkedMembers.indexOf(membersCount) > - 1
                ) {
                let names = allNames.split("|").filter(elm => elm.includes(keywords));
                // the name of band is tha same of the author
                names = [...new Set(names)] // eliminate duplicates in an array
                for (name of names) {
                    let opt = document.createElement('option');
                    opt.value = capitalize(`${name} - ${authorMemb} / ${nameGroup}`);
                    opt.className = "opt-artist"
                    artistName.appendChild(opt);
                }
                elm.style.display = "block";
            } else {
                elm.style.display = "none";
            }
        });
    }, 500);
}

function capitalize(str) {
    return str.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase());
}
