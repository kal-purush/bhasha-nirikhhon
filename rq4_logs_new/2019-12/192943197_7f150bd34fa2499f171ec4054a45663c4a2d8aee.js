//<td>Away</td>
//<td><a class="button" href="http://{{.IpPort}}/heating/temporary/max/12">Click Here</a></td>


function setTemporary(type) {
    valueDay = document.getElementById('day').value;
    valueHour = document.getElementById('hour').value;
    if (valueDay !=="" ) {
        valueHour = valueHour + valueDay * 24
    }
    ipPort = document.getElementById('ipPort').value;
    url = 'http://' + ipPort + '/heating/temporary/' + type +'/' +valueHour;
    window.location.assign( url);
}