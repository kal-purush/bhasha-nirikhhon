let comments = [];


document.querySelector('#status').addEventListener('change', async (e)=>{
    let statusValue = e.target.value;
    comments = await loadFilterComments(statusValue);
    renderComments(comments);

});
