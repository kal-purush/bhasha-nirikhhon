let button = document.getElementById('values');
const getValue= (button) => {
    button.addEventListener('click', 
    function (event) {
        let vall = event.target.attributes[0].value;
        let  disp = document.getElementById("disp");
        disp.value = disp.value + vall;
        let ans =0
        if (vall=="C") {
            disp.value = "";
        }
        else if(vall === "="){
            ans = disp.value.substring(0, disp.value.length - 1);
            try {
                disp.value = eval(ans);
            } catch (error) {
                disp.value = "Invalid";
            }
        }
})
}




getValue(button);
// dispValue(getValue);

// 22178987
// 69072
// 6624
// SP986924