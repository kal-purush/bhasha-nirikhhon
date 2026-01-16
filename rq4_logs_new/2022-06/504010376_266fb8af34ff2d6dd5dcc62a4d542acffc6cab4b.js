const allSkeleton = document.querySelectorAll('.placeholder')

        window.addEventListener('load', function() {
            allSkeleton.forEach(item => {
                item.classList.remove('placeholder')
            })
        })