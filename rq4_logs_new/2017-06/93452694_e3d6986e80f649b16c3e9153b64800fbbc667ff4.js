/**
 * File Name: main.js
 * Description: Main javascript file that contains all methods for manipulating
 *      what the iframes will display, the dimensions of each iframe, and the
 *      total number of iframes that will be displayed.
 */

window.onload = init;


/**
 * init - Method for assigning event methods for various
 *      DOM elements
 * @return null
 */
function init(){
    // URL Input
    var urlInput = document.getElementById("url-input");

    // Radio Buttons
    var widthRadioBtn = document.getElementById("width");
    var deviceRadioBtn = document.getElementById("device");
    widthRadioBtn.onclick = toggleOptionDisplay;
    deviceRadioBtn.onclick = toggleOptionDisplay;
} // End of init


/**
 * toggleOptionDisplay - Toggle the visibility of the
 *      device display options
 * @return {[type]} [description]
 */
function toggleOptionDisplay(){
    // Div holding the device display options
    var displayOptions = document.getElementById("deviceOptions");

    if(this.value === "width"){
        // Hide on width based testing
        displayOptions.style.display = "none";
    }else{
        // Show on device-based testing
        displayOptions.style.display = "block";
    }
} // End of toggleOptionDisplay