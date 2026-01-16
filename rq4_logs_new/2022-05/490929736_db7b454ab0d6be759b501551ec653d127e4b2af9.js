//BASE URL
const baseUrl = 'https://random-data-api.com/api/'; //https://random-data-api.com/

//Generate Random Users
const getRandomUsers = async () => {
    try {
        const response = await axios.get(`${baseUrl}users/random_user?size=22`);
        return response.data;
    } catch (err) {
        console.log('ERROR', err);
    }
}

const listElement = document.getElementById('list');
const pageNumbersElement = document.getElementById('pagination');
let listItems = await getRandomUsers();
let currentPage = 1;
let displayedRows = 5;

//SETUP NAMES TO DISPLAY
const displayList = (items, wrapper, rowsPerPage, page) => {
    //Setup an empty wrapper to avoid overlap
    wrapper.innerHTML = "";

    //Page Starts at 1, but Array Is 0 Indexed (want to subtract)
    page--;

    let loopStart = rowsPerPage * page; 
    let loopEnd = loopStart + rowsPerPage;
    let paginatedItems = items.slice(loopStart, loopEnd);

    //ITERATE THROUGH THE PAGINATED ITEMS ARRAY
    for(let i = 0; i < paginatedItems.length; i++){
        let item = paginatedItems[i].first_name;

        let itemElement = document.createElement('div');
        itemElement.classList.add('item');
        itemElement.innerText = item;

        wrapper.appendChild(itemElement);
    }
}

//SETUP PAGINATION CONTAINER
const paginationSetUp = (items, wrapper, rowsPerPage) => {
    wrapper.innerHTML = "";

    //Need to Round Up to get last few items
    let pageCount = Math.ceil(items.length / rowsPerPage);

    for(let i = 1; i < pageCount + 1; i++){
        let btn = paginationButton(i, items);
        wrapper.appendChild(btn)

    }
}

//GENERATE PAGINATION BTN
const paginationButton = (page, items) => {
    let paginationBtn = document.createElement('button');
    paginationBtn.innerText = page;

    if(currentPage === page){
        paginationBtn.classList.add('active');
    }

    paginationBtn.addEventListener('click', () => {
        currentPage = page;
        displayList(items, listElement, displayedRows, currentPage)

        let currentBtn = document.querySelector('.pagenumbers button.active');
        currentBtn.classList.remove('active');

        paginationBtn.classList.add('active');
    })

    return paginationBtn;
}

displayList(listItems, listElement, displayedRows, currentPage);
paginationSetUp(listItems, pageNumbersElement, displayedRows);