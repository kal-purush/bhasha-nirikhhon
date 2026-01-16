document.addEventListener('DOMContentLoaded', function(){
    function main(): void {
        const categories = [...document.querySelectorAll('.categories-list-item')];
        
        for (let category of categories){
            category.addEventListener('mouseover', function(){
                const subcategories = this.querySelectorAll('.subcategories-list-item');
                this.classList.add('active-category');
                for (let subcategory of subcategories){
                    subcategory.classList.add('active');
                }

            });
            category.addEventListener('mouseleave', function(){
                const subcategories = this.querySelectorAll('.subcategories-list-item');
                this.classList.remove('active-category');
                for (let subcategory of subcategories){
                    subcategory.classList.remove('active');
                }
            });
        }
    }
    main();
});