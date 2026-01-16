document.querySelectorAll('.btn-edit').forEach(function(btn) {
    btn.addEventListener('click', function(e) {
        let item = e.target.closest('.item');
        let children = item.children;
        console.log('children are:');
        console.log(children);
        let id = children[0].textContent;
        let name = children[1].textContent;
        let email = children[2].textContent;
        let phone = children[3].textContent;
        let adress = children[4].textContent;
        console.log('test');
        document.querySelector('#exampleModaledit .id').value = id;
        document.querySelector('#exampleModaledit .name').value = name;
        document.querySelector('#exampleModaledit .email').value = email;
        document.querySelector('#exampleModaledit .phone').value = phone;
        document.querySelector('#exampleModaledit .adress').value = adress;


    })

})