window.addEventListener("load", function() {
    let contentOne = document.getElementById("contentOne");
    let contentTwo = document.getElementById("contentTwo");
    let contentThree = document.getElementById("contentThree");
    let contentFour = document.getElementById("contentFour");
    let contentFive = document.getElementById("contentFive");

    contentOne.style.display = 'none';
    contentTwo.style.display = 'none';
    contentThree.style.display = 'none';
    contentFour.style.display = 'none';
    contentFive.style.display = 'none';
});

function toggleContent(currentHeading, currentContent) {
    if (currentContent.style.display === 'none') {
        currentHeading.style.fontWeight = 'bold';
        currentContent.style.display = 'block';
    } else {
        currentHeading.style.fontWeight = 'normal';
        currentContent.style.display = 'none';
    }
};