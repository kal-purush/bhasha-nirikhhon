function resetForm() {
    document.getElementById('minValue').disabled = false;
    document.getElementById('maxValue').disabled = false;
    var table = document.getElementById("listOne");
    var table2 = document.getElementById("listTwo");
    table.innerHTML = "";
    table2.innerHTML = "";
    document.forms["myForm"]["minValue"].value = "";
    document.forms["myForm"]["maxValue"].value = "";
    document.forms["myForm"]["myNumber"].value = "";
    document.getElementById("meanValue").value = "";
}

function disableMin() {
    //alert ("mouse leave min value");
    document.getElementById('minValue').disabled = true;
}

function disableMax() {
    //alert ("mouse leave min value");
    document.getElementById('maxValue').disabled = true;
}


function calcValues() {
    document.getElementById('minValue').disabled = true;
    document.getElementById('maxValue').disabled = true;
    var minValue = document.forms["myForm"]["minValue"].value;
    var maxValue = document.forms["myForm"]["maxValue"].value;
    var myNumber = document.forms["myForm"]["myNumber"].value;
    //alert ("min a" + minValue + "  max " + maxValue + "  myNumber " + myNumber);
    minValue = parseInt(minValue);
    maxValue = parseInt(maxValue);
    myNumber = parseInt(myNumber);

    if (minValue > maxValue) {
        alert("Minimum value must be less than maximum value");
        return;
    }
    if (maxValue == minValue) {
        alert("Maximum value must be more than mimimum value");
        return;
    }
    //alert ("min b" + minValue + "  max " + maxValue + "  myNumber " + myNumber);
    if (myNumber < minValue) {
        alert("Your number is less than the minumum");
        return;
    }
    //alert ("min c " + minValue + "  max " + maxValue + "  myNumber " + myNumber);
    if (myNumber > maxValue) {
        alert("Your number is greater than the maximum");
        return;
    }
    //alert ("min d " + minValue + "  max " + maxValue + "  myNumber " + myNumber);


    // save the numbers entered
    var myTable = document.getElementById("listOne");
    (myTable.insertRow()).innerHTML = myNumber;

    // load up result area
    document.getElementById("meanValue").value   = myMean();
    document.getElementById("modeValue").value   = myMode();
    document.getElementById("medianValue").value = myMedian();
    
}


function myMean() {
    //  calculate the average of what's entered
    var myTable = document.getElementById("listOne");
    //alert ("got to here " + myTable);    

    var rows = myTable.rows;
    //alert ("got to here a" );
    let myTotal = 0;

    for (i = 0; i < rows.length; i++) {
        myTotal = myTotal + parseInt(rows[i].innerHTML);
    }
    //alert ( "avg is..."  + myTotal / rows.length);
    return myTotal / rows.length;

}


function myMode() {
    // as result can be bimodal or multi-modal,
    // the returned result is provided as an array
    // mode of [3, 5, 4, 4, 1, 1, 2, 3] = [1, 3, 4]
    var myTable = document.getElementById("listOne");
    var rows = myTable.rows;

    var modes = [], count = [], i, number, maxIndex = 0;
    //alert ("mode:  got to here a")
    for (i = 0; i < rows.length; i += 1) {
        number = parseInt(rows[i].innerHTML);
        count[number] = (count[number] || 0) + 1;
        if (count[number] > maxIndex) {
            maxIndex = count[number];
        }
    }
    for (i in count)
        if (count.hasOwnProperty(i)) {
            if (count[i] === maxIndex) {
                modes.push(Number(i));
            }
        }
    return modes;
}



function myMedian() {
    var myTable = document.getElementById("listOne");
    var rows = myTable.rows;
  
    sortTable();
    alert("mymeedian:  got to here  a " );
    var myTable = document.getElementById("listOne");
    var rows = myTable.rows;
    
    //var sorted = document.getElementById("listOne").slice();  //.sort((a, b) => a - b);
    const middle = Math.floor(rows.length / 2);
  
    if (rows.length % 2 === 0) {
        return (parseInt(rows[middle - 1].innerHTML) + parseInt(rows[middle].innerHTML) / 2);
    }

    alert("mymeedian:  got to here  b " + parseInt(rows[middle].innerHTML));
    return parseInt(rows[middle].innerHTML);

}



function sortTable() {

    var table, rows, switching, i, x, y, shouldSwitch;
    table = document.getElementById("listOne");
    switching = true;
    //alert("sorttable:  got to here  a " );
    /* Make a loop that will continue until
    no switching has been done: */
    while (switching) {
        // Start by saying: no switching is done:
        switching = false;
        rows = table.rows;
        //alert("sorttable:  got to here  b " + rows.length);
        /* Loop through all table rows (except the
        first, which contains table headers): */
        for (i = 1; i < (rows.length - 1); i++) {
            // Start by saying there should be no switching:
            shouldSwitch = false;
            /* Get the two elements you want to compare,
            one from current row and one from the next: */
            x = parseInt(rows[i].innerHTML)
            //x = rows[i].getElementsByID("listOne").innerHTML[0];
            //alert("sorttable:  got to here  x " + x);
            y = parseInt(rows[i + 1].innerHTML)
            //y = rows[i + 1].getElementsByID("listOne").innerHTML[0];
            //alert("sorttable:  got to here  y " + y);
            // Check if the two rows should switch place:
            if (x.innerHTML > y.innerHTML) {
                // If so, mark as a switch and break the loop:
                shouldSwitch = true;
                break;
            }
        }

        if (shouldSwitch) {
            /* If a switch has been marked, make the switch
            and mark that a switch has been done: */
            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
            switching = true;
        }
    }
}



/*
    if (theWord <= " ") {
        alert("A word must be entered");
    }
    if (theCol == null) {
        alert("You must choose a list   ");
    }
    if (theCol < 1) {
        alert("the list must be either 1 or 2");
    }
    if (theCol > 2) {
        alert("the list must be either 1 or 2");
    }

    if (theCol == 1) {
        myResult = theWord + " " + palindrome1(theWord);
        var table = document.getElementById("listOne");
        (table.insertRow()).innerHTML = myResult;
    }

    if (theCol == 2) {
        var thisWord = reverseTheString(theWord);

        myResult = theWord + " " + false;
        if (theWord == thisWord) {
            myResult = theWord + " " + true;
        }
        var table = document.getElementById("listTwo");
        (table.insertRow()).innerHTML = myResult;
    }

} */

/* function palindrome1(theWord) {
    //  this reverses the word and returns a boolean if
    //  parm = reverse word
    thisWord = theWord;
    var thisWord = theWord.split("").reverse().join("");
    if (theWord == thisWord) {
        return true;
    }
    if (theWord != thisWord) {
        return false;
    }
}

function reverseTheString(theWord) {
    //  this reverses a string working backward char by char
    //  appending to the end of the string
    var returnVal = "";
    for (var i = theWord.length - 1 ; i >= 0; i--) {
        returnVal += theWord[i];
    }
    return returnVal;
} */
