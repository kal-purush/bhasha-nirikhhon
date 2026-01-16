export default function setCategoryList(selectedChairs) {
    
    let selectedChairsCategory = selectedChairs.map(chair =>(chair.category));

    return selectedChairsCategory.filter((elem, pos, arr) => {
        return arr.indexOf(elem) === pos;
    }).sort();
}
