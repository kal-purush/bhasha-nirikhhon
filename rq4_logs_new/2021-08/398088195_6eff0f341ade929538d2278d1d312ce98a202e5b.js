window.addEventListener('load', () => {
    let smallCups = document.querySelectorAll(".cup-small"),
        liters = document.getElementById("liters"),
        percentage = document.getElementById("percentage"),
        remained = document.getElementById("remained");

        updateBigCup();

        smallCups.forEach((cup,index)=>{
            cup.addEventListener('click', () => {
                yorituvchiCups(index);
            })
        })


        function yorituvchiCups(index){
            const a = smallCups[index].classList.contains('full');
            const b = !smallCups[index].nextElementSibling.classList.contains('full');
            if(a && b){
                index--;
            }

            smallCups.forEach((cup, index2)=> {
                (index2 <= index)?
                cup.classList.add('full'):
                cup.classList.remove('full');
            })

            updateBigCup();
        }


        function updateBigCup() {
            const fullCups = document.querySelectorAll(".cup-small.full").length;
            const totalCups = smallCups.length;

            if(fullCups === 0 ){
                percentage.style.visibility = 'hidden';
                percentage.style.height = 0;
            } else {
                percentage.style.visibility = 'visible';
                percentage.style.height = `${fullCups / totalCups * 330}px`;
                percentage.innerText = `${fullCups / totalCups * 100}%`;
            }


            if(fullCups === totalCups){
                remained.style.visibility = 'hidden';
                remained.style.height = 0;
            } else {
                remained.style.visibility = 'visible';
                liters.innerText = `${2 - ( 250 * fullCups / 1000)}L`
            }




            console.log(fullCups);
            console.log(totalCups);
        }
        


    console.log(smallCups);
    console.log(liters);
    console.log(percentage);
    console.log(remained);
})