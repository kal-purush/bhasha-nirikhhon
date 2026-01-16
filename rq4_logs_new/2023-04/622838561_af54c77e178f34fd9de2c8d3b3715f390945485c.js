const categoryLargeScreenFunction = () => {

    let generalCategory = document.querySelectorAll('.category-container.large-screen .category.general');
    for(let i = 0; i < generalCategory.length; i++){
        (function(index){
            if(generalCategory[index].querySelector('.sub-menu')){
                generalCategory[index].querySelector('.sub-menu').classList.add('d-none');
            }
        })(i);
    }


    let generalCategoryName = document.querySelectorAll('.category-container.large-screen .category.general .name.general');
    let generalCategorySubmenu = document.querySelectorAll('.category-container.large-screen .category.general .sub-menu.general');

    for(let i = 0; i < generalCategoryName.length; i++){
        (function(index){

            generalCategoryName[i].addEventListener('mouseover',function(){
                generalCategorySubmenu[index].classList.remove('d-none');
            });
            generalCategoryName[i].addEventListener('mouseout',function(){
                generalCategorySubmenu[index].classList.add('d-none');
            });
            generalCategorySubmenu[i].addEventListener('mouseover',function(){
                generalCategorySubmenu[index].classList.remove('d-none');
            });
            generalCategorySubmenu[i].addEventListener('mouseout',function(){
                generalCategorySubmenu[index].classList.add('d-none');
            });
        })(i);
    }

    let subCategory = document.querySelectorAll('.category-container.large-screen .sub-menu.general .category');

    for(let i = 0; i < subCategory.length; i++){
        (function(index){
            if(subCategory[index].querySelector('.alt-menu')){
                subCategory[index].querySelector('.name').classList.add('enable');
                subCategory[index].querySelector('.alt-menu').classList.add('enable');
                subCategory[index].querySelector('.alt-menu').classList.add('d-none');
            }else{
                subCategory[index].querySelector('.name button').remove();
            }
        })(i);
    }



    let enableButton = document.querySelectorAll('.category-container.large-screen .category .name.enable button');
    let enableAltMenu = document.querySelectorAll('.category-container.large-screen .category .alt-menu.enable');
    
    for(let i = 0; i < enableButton.length; i++){
        (function(index){
            enableButton[i].addEventListener('click',function(){
                enableButton[index].classList.toggle('active');
                enableAltMenu[index].classList.toggle('d-none');
            })
        })(i)
    }
    


}

export default categoryLargeScreenFunction;