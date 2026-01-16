document.getElementById("open-model-btn").addEventListener("click" ,function(){
    document.getElementById("my-modal").classList.add("open")
})
document.getElementById("closed-my-modal-btn").addEventListener("click" ,function(){
    document.getElementById("my-modal").classList.remove("open")
})
var element = document.getElementById('phone');
var maskOptions = {
    mask: '+7(000)000-00-00',
    lazy: false
} 
var mask = new IMask(element, maskOptions);