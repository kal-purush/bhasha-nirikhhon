document.addEventListener("DOMContentLoaded", function(){
    const textE = document.getElementById('typetext');
    const text = 'There is something strange...';
    let index = 0;

    function typemachine(){
        if(index < text.length) {
            textE.innerHTML += text.charAt(index);
            index++;
            setTimeout(typemachine, 100);

        }
        else {
            setTimeout(() => {
                textE.innerHTML = '';
                index = 0;
                typemachine();
            }, 100)
        }
    }
    typemachine();
}
)


document.addEventListener('DOMContentLoaded', function() {
    const hoverAreas = document.querySelectorAll('.hover_area');
    const strangePages = [
        './maze.html',
        './typist.html'
    ]
    const info = [
        'She is certainly a freak, 10 days a week',
        'Hit that SPOT!',
        'Damn i really make it look EASY'
    ]

    hoverAreas.forEach(area => {
        const hoverImage = area.querySelector('img');

        area.addEventListener('mouseenter', function() {
            hoverImage.style.display = 'block';
        });

        area.addEventListener('mouseleave', function() {
            hoverImage.style.display = 'none';
        });

        hoverImage.addEventListener('click', function() {
            const randomIndex = Math.floor(Math.random()*strangePages.length);
            const randomInfoIndex = Math.floor(Math.random()*info.length);
            const randomPage = strangePages[randomIndex];

            alert(info[randomInfoIndex]);
            
            window.location.href = randomPage;
        });
    });
});
