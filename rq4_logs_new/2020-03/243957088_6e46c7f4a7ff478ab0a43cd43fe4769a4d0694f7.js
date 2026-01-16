document.addEventListener('scroll', onScroll);

function onScroll(event) {
    const curPos = window.scrollY + 200;
    const sects = document.querySelectorAll('#wrapper>section');
    const links = document.querySelectorAll('nav a');

    console.log(document.querySelectorAll('#wrapper>section'));

    sects.forEach((el) => {
         console.log(el.offsetTop);
        if (el.offsetTop < curPos && (el.offsetTop + el.offsetHeight) >= curPos) {
            links.forEach((a) => {
                a.classList.remove('nav-text_active');
                if (el.getAttribute('id') === a.getAttribute('href').substring(1)) {
                    a.classList.add('nav-text_active');
                }
            })
        }
    })
}