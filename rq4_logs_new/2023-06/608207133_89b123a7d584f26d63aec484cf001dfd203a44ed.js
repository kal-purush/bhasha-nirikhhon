const deleteBtn = document.getElementById('delete-btn');

const crosses = [];
let i = 1;
let cross = document.getElementById(`cross-${i}`);

while (cross) {
  crosses.push(cross);
  i++;
  cross = document.getElementById(`cross-${i}`);
}

crosses.forEach((cross) => {
  cross.addEventListener('click', () => {
    event.preventDefault();
    cross.classList.toggle('opacity-100');

    if (cross.classList.contains('opacity-100')) {
        deleteBtn.classList.remove('hidden');
      } 
    else {

        let isBtnVisible = false; 

        crosses.forEach((cross) => {
            if (cross.classList.contains('opacity-100')) {
                isBtnVisible = true;
            }
        })

        if (!isBtnVisible)
        deleteBtn.classList.add('hidden');
        }
    });
});
  
