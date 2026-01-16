/**
 * 
 *  Document     : control.js
 *  Created on   : Jan 26, 2017, 4:35:25 AM
 *  Author       : Subir Sekhar
 *  Co-Author    : Pabitra K Jana
 *  Organization : IIT Khatagpur
 *  
 */


var U1 = null;
var V11 = null;
var V2;
var bv = null;
var bv_ind = null;
var pulse_arr;
var loop;
var clicks = 0;
var row;
var table;
var rowCount;
var voltagePerstage;
//document.getElementById("flashVoltage").value = volt;





function trigger_generator() {

//    clicks += 1;
//        document.getElementById("demo").innerHTML = clicks;
//    console.log(clicks);


    //alert(ipBox);

    window.setTimeout(function () {
        rotateObject("cylinder38", 0, 90);
        rotateObject("cylinder39", 0, 90);
    }, 500);

    window.setTimeout(function () {
        moveObject("sphere5", -3.0, -3.0, 2.3, 2.3, 3.55, 3.7);
        moveObject("sphere7", -3.1, -3.1, 0.7, 0.7, 3.55, 3.7);
        moveObject("sphere9", -3.15, -3.15, -0.7, -0.7, 3.55, 3.7);
        moveObject("cylinder26", -0.5, 1, 0, 0, 1, 1);
        moveObject("cylinder27", -0.55, 1, 0, 0, 1, 1);
        moveObject("cylinder28", -0.4, 1, 0, 0, 1, 1);
        moveObject("tcylinder26", -3.3, -3.3, -0.65, -0.65, 3.32, 3.32);
        moveObject("tcylinder27", -3.3, -3.3, 0.75, 0.75, 3.32, 3.32);
        moveObject("tcylinder28", -3.2, -3.2, 2.25, 2.25, 3.32, 3.32);
        moveObject("cylinder15", -3.69, -3.6, 0.65, 0.65, 3.15, 3.15);

    }, 1000);
    window.setTimeout(function () {
        moveObject("sphere5", -3.0, -3.0, 2.3, 2.3, 3.45, 3.7);
        moveObject("sphere7", -3.1, -3.1, 0.7, 0.7, 3.45, 3.7);
        moveObject("sphere9", -3.15, -3.15, -0.7, -0.7, 3.45, 3.7);
        moveObject("cylinder26", -0.3, 1, 0, 0, 1, 1);
        moveObject("cylinder27", -0.3, 1, 0, 0, 1, 1);
        moveObject("cylinder28", -0.25, 1, 0, 0, 1, 1);
        moveObject("tcylinder26", -3.2, -3.2, -0.65, -0.65, 3.32, 3.32);
        moveObject("tcylinder27", -3.3, -3.3, 0.75, 0.75, 3.32, 3.32);
        moveObject("tcylinder28", -3.15, -3.15, 2.25, 2.25, 3.32, 3.32);
        moveObject("cylinder15", -3.67, -3.6, 0.65, 0.65, 3.15, 3.15);
    }, 1500);
    window.setTimeout(function () {
        moveObject("sphere5", -3.0, -3.0, 2.3, 2.3, 3.35, 3.7);
        moveObject("sphere7", -3.1, -3.1, 0.7, 0.7, 3.35, 3.7);
        moveObject("sphere9", -3.15, -3.15, -0.7, -0.7, 3.35, 3.7);
        moveObject("cylinder26", -0.2, 1, 0, 0, 1, 1);
        moveObject("cylinder27", -0.2, 1, 0, 0, 1, 1);
        moveObject("cylinder28", -0.2, 1, 0, 0, 1, 1);
        moveObject("tcylinder26", -3.1, -3.1, -0.65, -0.65, 3.2, 3.2);
        moveObject("tcylinder27", -3.3, -3.3, 0.75, 0.75, 3.2, 3.2);
        moveObject("tcylinder28", -3.1, -3.1, 2.25, 2.25, 3.2, 3.2);
        moveObject("cylinder15", -3.65, -3.6, 0.65, 0.65, 3.15, 3.15);
    }, 2000);
    window.setTimeout(function () {
        moveObject("sphere5", -3.0, -3.0, 2.3, 2.3, 3.25, 3.7);
        moveObject("sphere7", -3.1, -3.1, 0.7, 0.7, 3.25, 3.7);
        moveObject("sphere9", -3.15, -3.15, -0.7, -0.7, 3.25, 3.7);
        moveObject("cylinder26", -0.1, 1, 0, 0, 1, 1);
        moveObject("cylinder27", -0.1, 1, 0, 0, 1, 1);
        moveObject("cylinder28", -0.1, 1, 0, 0, 1, 1);
        moveObject("tcylinder26", -3.1, -3.1, -0.65, -0.65, 3.2, 3.2);
        moveObject("tcylinder27", -3.3, -3.3, 0.75, 0.75, 3.2, 3.2);
        moveObject("tcylinder28", -3.1, -3.1, 2.25, 2.25, 3.2, 3.2);
        moveObject("cylinder15", -3.65, -3.6, 0.65, 0.65, 3.15, 3.15);
        colorChange("myFlags11");
        colorChange("myFlags12");
        colorChange("myFlags13");
        colorChange("myFlags15");
        colorChange("myFlags16");
    }, 2500);
    window.setTimeout(function () {
        moveObject("sphere13", -3, -2.95, -0.75, -0.75, 3.4, 3.4);
        moveObject("sphere14", -2.8, -2.75, 2.25, 2.25, 3.4, 3.4);
        moveObject("sphere15", -2.85, -2.7, 0.6, 0.6, 3.4, 3.4);
//        if(bv_ind != 0) {
//            var loop = bv_ind * 2;
//            var mm = 0;
//            for(mm=0; mm<loop; mm++)flashIvg();
//        }

        flashIVG();

        //moveObject("multi2", -9, -10, -0.75, -0.75, 4.5, 4.5);
    }, 2600);
    window.setTimeout(function () {
        moveObject("sphere13", -2.7, -2.7, -0.75, -0.75, 3.6, 3.6);
        moveObject("sphere14", -2.5, -2.5, 2.25, 2.25, 3.55, 3.55);
        moveObject("sphere15", -2.6, -2.6, 0.6, 0.6, 3.55, 3.55);

        moveObject("sphere16", 6.65, 6.65, 0.7, 0.7, 2.4, 2.4);

    }, 3500);
    window.setTimeout(function () {
        colorChange("myFlags1");
        colorChange("myFlags2");
        colorChange("myFlags3");
        colorChange("myFlags4");
        colorChange("myFlags5");
        colorChange("myFlags6");
        colorChange("myFlags7");
        colorChange("myFlags8");
        colorChange("myFlags9");
        colorChange("myFlags11");
        colorChange("myFlags12");
        colorChange("myFlags13");
        colorChange("myFlags15");
        colorChange("myFlags16");
        rotateObject("cylinder38", 0, 0);
        rotateObject("cylinder39", 0, 0);
        colorChange("myFlags10");
        chart.clearChart();
    }, 3700);
    window.setTimeout(function () {
        moveObject("sphere5", -3.05, -3.05, 2.3, 2.3, 3.7, 5);
        moveObject("sphere7", -3.15, -3.15, 0.7, 0.7, 3.7, 4);
        moveObject("sphere9", -3.2, -3.2, -0.7, -0.7, 3.7, 4);
        moveObject("cylinder26", -0.9, 1, 0, 0, 1, 1);
        moveObject("cylinder27", -1, 1, 0, 0, 1, 1);
        moveObject("cylinder28", -0.75, 1, 0, 0, 1, 1);
        moveObject("tcylinder26", -3.5, -3.5, -0.65, -0.65, 3.4, 3.4);
        moveObject("tcylinder27", -3.45, -3.45, 0.75, 0.75, 3.4, 3.4);
        moveObject("tcylinder28", -3.3, -3.3, 2.25, 2.25, 3.4, 3.4);
        moveObject("cylinder15", -3.75, -3.75, 0.65, 0.65, 3.15, 3.15);

        graphGenerator();

    }, 4000);
//    document.getElementById("buttn6").disabled = false;
    window.setTimeout(function () {


        document.getElementById("meter").hidden = true;

    }, 3400);
//alert(volt);

    window.setTimeout(function () {
        displayMap();
        myFunction();
    }, 4500);

    selectEnable();
    document.getElementById("buttn3").disabled = true;
    document.getElementById("buttn").disabled = true;

}

function displayMap() {
    document.getElementById('graph').style.display = "block";
    graphGenerator();
    table = document.getElementById("mainTable");
    rowCount = table.rows.length;
    if ((rowCount === 8) && (clicks === 10)) {
        document.getElementById("buttn7").disabled = false;
//        document.getElementById("sphereDiameter").disabled = false;
//        document.getElementById("sphereGap").disabled = false;
        document.getElementById("flashVoltage").value = volt.toFixed(2);
    }
//    if(rowCount ===3 ){
//    alert("rowcount =" + rowCount);
//    }
}

function graphGenerator() {
    var Rf = 115;
    var Rt = 15;
    var Cg = 0.001;
    var C1 = 0.6;
    var V0 = 50;
    var d = 7;
    var V = [];
    var t = [];
    var z = [];
    var RF = Rt;
    var RT = Rf;
    var a = 1 / ((Cg + C1) * RT + (RF * Cg));
    var a1 = 1000000 / ((Cg + C1) * RT + (RF * Cg));
    var b = ((Cg + C1) * RT + (RF * Cg)) / (RF * RT * Cg * C1);
    var r = 7.5;
    U = ((27.2 * 0.98 * 4 * r * ((1 + (0.54 / Math.sqrt(0.98 * r))) * (d / r))) / ((d / r) + 1 + Math.sqrt((((d / r) + 1) * ((d / r) + 1)) + 8)));
    for (var i = 0; i < 200; i++)
    {
        t[i] = i;
        V1 = (((3 * volt * 3.8 * 100) * (Math.exp(-a * t[i]) - Math.exp(-b * t[i]))) / (RF * C1 * (b - a)));
        V2 = (((3 * volt * 3.8 * 100) * (Math.exp(-a1 * t[i]) - Math.exp(-b * t[i]))) / (RF * C1 * (b - a1)));
        V11 = document.getElementById("breakdownVoltage").value;
        U1 = volt;

        var index = $.inArray(clicks, pulse_arr);
        if (index !== -1) {
            V[i] = V2;

        } else {
            V[i] = V1;
        }
//        if (V11 >= U1)
//        {
//            V[i] = V2;
//        } else
//        {
//            V[i] = V1;
//        }
    }
    z[i] = "[" + t[i] + " ," + V[i] + "]";
    var len = data.getNumberOfRows(); // ET NUMBER OF ROWS IN ARRAY 
    if (len > 0) {
        data.removeRows(0, len); // REMOVE THE PREVIOUS DATA
    }
    for (var i = 0; i < t.length; i++)
        data.addRow([t[i], V[i]]);
    //alert("After add: "+data.);
    var options = {
        title: 'V - T Curve',
        explorer: {maxZoomOut: 20},
        displayAnnotations: true,
        showArea: true,
        curveType: 'function',
        legend: {position: 'none'},
        'isStacked': true,
        pieSliceText: 'percentage',
//        tooltip: {isHtml: true},
//        selectionMode: 'multiple',
//        tooltip: {trigger: 'both'},
//        aggregationTarget: 'none',
        

        chartArea: {
            left: "5%",
            top: "8%",
            height: "80%",
             backgroundColor: '#000000',
              is3D: true,
            width: "90%"
        },
        hAxis: {
            title: 'Time ( Micro Second )',
             gridlines: {color: '#444444'}
        },
        vAxis: {
            title: 'Voltage ( kV )',
            gridlines: {color: '#444444'}
        },
        colors: ['#ffffff', '#097138'],
                selectionMode: 'multiple',
        tooltip: {trigger: 'both',
            focusTarget: 'category'
        },
        aggregationTarget: 'none',
        crosshair: {
            color: '#000',
            trigger: 'both',

            focused: {color: ['#00ff00']},
            selected: {color: ['#00ffff']},
            focusTarget: 'category',
            sortColumn: 0,
            sortAscending: false,
            sort: 'enable'
        }
//        crosshair: {
//            color: '#000',
//            trigger: 'both',
//
//            focused: {color: ['#00ff00']},
//            selected: {color: ['#00ffff']}
//        }


    };
    chart.clearChart();
    chart.draw(data, options);

}

function audio() {
    var audio = new Audio('images/flash_sound1.mp3');
    audio.play();
}

function start() {

    if (options === null) {
        alert("Set Parameter Values first");
    } else {
        document.getElementById("buttn").value = "Restart";
        document.getElementById("buttn3").disabled = false;
//        document.getElementById("breakdownVoltage").disabled = true;
//        document.getElementById("sphereDiameter").disabled = true;
//        document.getElementById("sphereGap").disabled = true;
    }
}

function restart() {


    if (clicks === 10) {
        alert("please Change the voltage level");

    } else {
//        document.getElementById("buttn").value = "Start Experiment";
        document.getElementById("buttn3").disabled = true;
        document.getElementById('graph').style.display = "none";
        document.getElementById('graph1').style.display = "none";
         start();
//        document.getElementById('buttn').addEventListener('click', function () {
//            start();
//        });
    }
}

function toggleButton() {
    var bval = document.getElementById("buttn").value;
    if (bval === "Start Experiment") {
        start();

    } else {
//        window.location.href = window.location.href;
        restart();
    }

}

function rotateObject(id, p1, p2) {
    scene.getNode(id, function (myrotate) {

        var angle = p1;
        var angleInc = 15;
        scene.on("tick", function () {
            if (angle >= p2) {
                angleInc *= 0;
            }
            angle += angleInc;
            myrotate.setAngle(angle);
        });
    });
}

function moveObject(id, r1, r2, r3, r4, r5, r6) {
    scene.getNode(id,
            function (myTranslate) {
                var x = r1;
                var xInc = 0.01;
                var y = r3;
                var yInc = 0.01;
                var z = r5;
                var zInc = 0.01;
                scene.on("tick",
                        function () {
                            if (x <= r2) {
                                xInc *= 0.0;
                            }
                            x += xInc;
                            if (y <= r4) {
                                yInc *= 0.0;
                            }
                            y += yInc;
                            if (z <= r6) {
                                zInc *= 0.0;
                            }
                            z += zInc;
                            myTranslate.setXYZ({x: x, y: y, z: z});
                        });
            });
}

function colorChange(id) {
    scene.getNode(id, function (myFlags) {
        myFlags.setTransparent(!myFlags.getTransparent());
    });
}

function flashIvg() {

    V11 = document.getElementById("breakdownVoltage").value;
    U1 = volt;

    if (V11 < U1)
    {
        moveObject("sphere16", 6.65, 6.65, 0.7, 0.7, 2.4, 2.4);

        window.setTimeout(function () {
            alert("Withstand");
        }, 1000);


    } else
    {
        moveObject("sphere16", 6.65, 6.65, 1.1, 1.1, 2.5, 2.5);
        audio();
        window.setTimeout(function () {
            alert("Flash Over");
        }, 1000);
    }
}



function rotateObject(id, p1, p2) {
    scene.getNode(id, function (myrotate) {

        var angle = p1;
        var angleInc = 15;
        scene.on("tick", function () {
            if (angle >= p2) {
                angleInc *= 0;
            }
            angle += angleInc;
            myrotate.setAngle(angle);
        });
    });
}




$(document).ready(function () {
    $(document).on("change", "#breakdownVoltage", function () {
        bv = $(this).val();
        bv_ind = $(this).find(":selected").attr("level");
        ///alert(bv+"    "+ind);
        UniqueRandom(bv_ind * 2);
    });
});


function UniqueRandom(flash_time) {
    pulse_arr = [];
    while (pulse_arr.length < flash_time) {
        var randomnumber = Math.ceil(Math.random() * 10);
        if (pulse_arr.indexOf(randomnumber) > -1)
            continue;
        pulse_arr[pulse_arr.length] = randomnumber;
    }
    console.log(pulse_arr);
//    loop = pulse_arr.length;
//
//    var mm = 0;
//    for (mm = 0; mm < loop; mm++)
//        pulseValue();


//var r = 2;
//var n=0;
//for(n=0;n<=pulse_arr.length;n++){
//if (r==pulse_arr[n]){
////document.getElementById("demo").innerHTML = cars[n];
//console.log(pulse_arr[n]);
//}
//}

}

//function pulseValue(loop) {
//    loop = pulse_arr.length;
//    var pickAValue = pulse_arr[Math.floor(Math.random())];
//
////            function () {
////        var todaysFruit = pulse_arr[Math.floor(Math.random())];
////        return todaysFruit;
////       
////    };
//    console.log(pickAValue);
//
//}

function flashIVG() {


    var index = $.inArray(clicks, pulse_arr);
    if (index !== -1) {
        moveObject("sphere16", 6.65, 6.65, 1.1, 1.1, 2.5, 2.5);
        audio();
//        window.setTimeout(function () {
//            alert("Flash Over");
//        }, 1000);

    } else {
        moveObject("sphere16", 6.65, 6.65, 0.7, 0.7, 2.4, 2.4);
//        window.setTimeout(function () {
//            alert("Withstand");
//        }, 1000);
    }


//    var n = 0;
//    for (n = 0; n <= pulse_arr.length; n++) {
    /*for (n in pulse_arr) {
     if (clicks === pulse_arr[n]) {
     
     moveObject("sphere16", 6.65, 6.65, 1.1, 1.1, 2.5, 2.5);
     
     audio();
     window.setTimeout(function () {
     alert("Flash Over");
     }, 1000);
     
     //document.getElementById("demo").innerHTML = cars[n];
     console.log(pulse_arr[n]);
     
     } else
     {
     moveObject("sphere16", 6.65, 6.65, 0.7, 0.7, 2.4, 2.4);
     
     
     window.setTimeout(function () {
     alert("Withstand");
     }, 1000);
     }
     //    break;
     }*/

}

function selectEnable() {
    if (clicks === 9) {
//        document.getElementById("breakdownVoltage").disabled = false;
//        alert("Please change the voltage level");
//        document.getElementById("sphereDiameter").disabled = true;
//        document.getElementById("sphereGap").disabled = true;
//        document.getElementById("buttn6").disabled = false;
        document.getElementById("buttn3").disabled = true;
    }
}


$(document).ready(function () {
    clicks = 0;
    function resetcounter() {
        clicks = 0;
//    document.getElementById("counting").value = clicks;
        console.log(clicks);
    }

    function count() {
        if (clicks < 10)
        {
            clicks += 1;
        } else
        {
            clicks = 1;
        }

        console.log(clicks);
    }
    $('#buttn3').click(
            function () {
                count();
            });

    $('#reset').click(
            function () {
                resetcounter();
            });
});



function myFunction() {

//    var r = document.getElementById("breakdownVoltage").value;
    var f = pulse_arr.length;
    var g = parseFloat(f / 10);

    var index = $.inArray(clicks, pulse_arr);
    var h = "W";
    if (index !== -1) {
        h = "F";
    }

    var allCells = row.cells;

//    allCells[0].innerHTML = r;

    allCells[clicks].innerHTML = h;

    if (clicks === 10) {
        allCells[11].innerHTML = g;

        window.setTimeout(function () {
            alert("Please change the voltage level");
            document.getElementById("breakdownVoltage").disabled = false;
        }, 1000);

    }

    document.getElementById("buttn6").disabled = true;
    document.getElementById("buttn").disabled = false;



}

function selectVoltage() {
    var sphere_diam = document.getElementById("sphereDiameter").value;
    var sphere_gap = document.getElementById("sphereGap").value;
    var ivgVoltage = document.getElementById("breakdownVoltage").value;

    clicks = 0;
    for (j in obj) {
        if (obj[j].sphere_diameter === sphere_diam) {
            for (i in obj[j].myObj) {

                if (obj[j].myObj[i].sphere_gap === sphere_gap) {
                    voltagePerstage = obj[j].myObj[i].Voltage;
                    var selectCount = document.getElementById("breakdownVoltage").options.length;
                    // alert(selectCount);
                    table = document.getElementById("mainTable");
                    rowCount = table.rows.length;
                    // alert(rowCount);
//                    if (rowCount === 7 && clicks === 10) {
//                        document.getElementById("buttn7").disabled = false;
//                        document.getElementById("sphereDiameter").disabled = true;
//                        document.getElementById("sphereGap").disabled = true;
//                    }

                    for (rowCount = 2; rowCount < selectCount; rowCount++) {
                        row = table.insertRow(rowCount);
                        for (s = 0; s < 12; s++) {
                            row.insertCell(s);
                        }


                        break;
                    }
//                     alert("you have select voltage level " +  voltagePerstage/3 + " kV");
                }

                //n = n + "  <option>" + obj[j].myObj[i].Voltage + "</option>";

            }
            break;

        }

    }
    document.getElementById("breakdownVoltage").disabled = true;
    document.getElementById("sphereDiameter").disabled = true;
    document.getElementById("sphereGap").disabled = true;
    clicks = 0;
    var source = document.getElementById("breakdownVoltage");
    source.options[source.selectedIndex].disabled = true;
    var r = document.getElementById("breakdownVoltage").value;
    var allCells = row.cells;
    allCells[0].innerHTML = r;
    //alert("ok");

}


function drawBasic() {
//    var ivgVoltage = document.getElementById("breakdownVoltage").value;
    table = document.getElementById("mainTable");
    var ipBox = document.getElementById("flashVoltage").value;
    var ipBox1 = document.getElementById("flashVoltage1").value;
    var rwe1 = table.rows[2].cells[0];
    var dt1 = rwe1.innerHTML;
    var rwe2 = table.rows[3].cells[0];
    var dt2 = rwe2.innerHTML;
    var rwe3 = table.rows[4].cells[0];
    var dt3 = rwe3.innerHTML;
    var rwe4 = table.rows[5].cells[0];
    var dt4 = rwe4.innerHTML;
    var rwe5 = table.rows[6].cells[0];
    var dt5 = rwe5.innerHTML;
    var rwe6 = table.rows[7].cells[0];
    var dt6 = rwe6.innerHTML;



    ///------------- 2nd column (Probability) -------//
    var rw1 = table.rows[2].cells[11];
    var dtt1 = rw1.innerHTML;
    var rw2 = table.rows[3].cells[11];
    var dtt2 = rw2.innerHTML;
    var rw3 = table.rows[4].cells[11];
    var dtt3 = rw3.innerHTML;
    var rw4 = table.rows[5].cells[11];
    var dtt4 = rw4.innerHTML;
    var rw5 = table.rows[6].cells[11];
    var dtt5 = rw5.innerHTML;
    var rw6 = table.rows[7].cells[11];
    var dtt6 = rw6.innerHTML;
//    var dtt7 = ((parseFloat(dtt1) + parseFloat(dtt2) + parseFloat(dtt3) + parseFloat(dtt4) + parseFloat(dtt5) + parseFloat(dtt6)) / 6);
//    var dt7 = ((parseFloat(dt1) + parseFloat(dt2) + parseFloat(dt3) + parseFloat(dt4) + parseFloat(dt5) + parseFloat(dt6)) / 6);

    var dt7 = ipBox;
    var dtt7 = ipBox1;

    var len = data1.getNumberOfRows(); // ET NUMBER OF ROWS IN ARRAY 
    if (len > 0) {
        data1.removeRows(0, len); // REMOVE THE PREVIOUS DATA
    }

    data1.addRows([
        [parseFloat(dt1), parseFloat(dtt1)],
        [parseFloat(dt2), parseFloat(dtt2)],
        [parseFloat(dt3), parseFloat(dtt3)],
        [parseFloat(dt7), parseFloat(dtt7)],
        [parseFloat(dt4), parseFloat(dtt4)],
        [parseFloat(dt5), parseFloat(dtt5)],
        [parseFloat(dt6), parseFloat(dtt6)]
    ]);

    var options = {
        title: 'Probability Curve',
        explorer: {maxZoomOut: 20},
        displayAnnotations: true,
        showArea: true,
        curveType: 'function',
        legend: {position: 'none'},
        'isStacked': true,
        pieSliceText: 'percentage',
        chartArea: {
            left: "10%",
            top: "10%",
            height: "70%",
            width: "80%"
        },
        hAxis: {
            title: 'Voltage Level ( kV ) '
        },
        vAxis: {
            title: 'Probability'
        },
        colors: ['#000000', '#097138'],
        selectionMode: 'multiple',
        tooltip: {trigger: 'both',
            focusTarget: 'category'
        },
        aggregationTarget: 'none',
        crosshair: {
            color: '#000',
            trigger: 'both',

            focused: {color: ['#00ff00']},
            selected: {color: ['#00ffff']},
            focusTarget: 'category',
            sortColumn: 0,
            sortAscending: false,
            sort: 'enable'
        }

    };

//    chart1 = new google.visualization.LineChart(document.getElementById('chartContainer'));
    data1.sort({column: 0, desc: true});
    chart1.draw(data1, options);
    chart1.setSelection([{row: 3, column: 1}]);
//    document.getElementById("graph1").style.visibility = "visible";
}

function displayChart() {
    document.getElementById('graph1').style.display = "block";
    drawBasic();
}