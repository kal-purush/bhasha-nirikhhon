// Toggle button 
let menu = document.querySelector('#menu');
let nav = document.querySelector('#nav');

menu.addEventListener('click', ()=>{
    if(nav.classList.contains('hidden')){
        nav.classList.remove('hidden');
    }else{
        nav.classList.add('hidden');
    }
});

//dept Module
let deptNav = document.querySelector('#deptNav')
let dept = document.querySelector('#dept')
let addDeptForm = document.querySelector('#addDeptForm')
let closeModalDept = document.querySelector('#closeModalDept')

    // dept Navigation 
    deptNav.addEventListener('click', ()=>{
        dept.classList.remove('hidden')
        addDeptForm.classList.remove('hidden')
    })

    // Close Modal 
    closeModalDept.addEventListener('click', ()=>{
        dept.classList.add('hidden')
    })

//Staff Module
let staffNav = document.querySelector('#staffNav')
let staff = document.querySelector('#staff')
let addStaffForm = document.querySelector('#addStaffForm')
let closeModalStaff = document.querySelector('#closeModalStaff')

    // Staff Navigation 
    staffNav.addEventListener('click', ()=>{
        staff.classList.remove('hidden')
        addStaffForm.classList.remove('hidden')
    })

    // Close Modal 
    closeModalStaff.addEventListener('click', ()=>{
        staff.classList.add('hidden')
    })

//Profile Module
let profileNav = document.querySelector('#profileNav')
let profile = document.querySelector('#profile')
let profileForm = document.querySelector('#profileForm')
let closeModalProfile = document.querySelector('#closeModalProfile')

    // Profile Navigation 
    profileNav.addEventListener('click', ()=>{
        profile.classList.remove('hidden')
        profileForm.classList.remove('hidden')
    })

    // Close Modal 
    closeModalProfile.addEventListener('click', ()=>{
        profile.classList.add('hidden')
    })