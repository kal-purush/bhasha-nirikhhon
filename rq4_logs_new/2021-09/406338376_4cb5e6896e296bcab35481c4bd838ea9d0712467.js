window.onload = () => {
    const scrollToTopEls = document.querySelectorAll('.scroll-to-top');

    if (scrollToTopEls.length > 0) {
        scrollToTopEls.forEach(el => {
            el.addEventListener('click', () => {
                window.scrollTo({
                    top: 0,
                    behavior: 'smooth'
                })
            })
        })
    }
}