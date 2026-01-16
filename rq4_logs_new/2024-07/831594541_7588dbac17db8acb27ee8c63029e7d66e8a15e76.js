const tierInput = document.getElementById('tier');
console.log(tierInput);

const submitBtn = document.getElementById('sumit');
submitBtn.addEventListener('click', (event) => {
    console.log("button is clicked");
    event.preventDefault();// stops the default behaviour of the event

    // to get access of the element on which this event was fired
    const target = event.target;
    console.log(tierInput.value);
    createTierList(tierInput.value);
});
function createTierList(tierListName) {
    const newTierList = document.createElement('div');
    newTierList.classList.add('tier-list');

    const heading = document.createElement('h1');
    heading.textContent = tierListName;
    newTierList.appendChild(heading);


     const newTierListitems = document.createElement('div');
     newTierListitems.classList.add('tier-list-items');  
     newTierList.appendChild(newTierListitems); 
    
     const newTierListitemsForLine = document.createElement('div');
     newTierListitemsForLine.classList.add('line');
     newTierList.appendChild(newTierListitemsForLine);


     const tierSection = document.getElementById
     ('tier-list-section');
     tierSection.appendChild(newTierList);
}