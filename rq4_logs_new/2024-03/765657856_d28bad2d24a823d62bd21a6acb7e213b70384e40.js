document.addEventListener('DOMContentLoaded', function () {
       var xhttp = new XMLHttpRequest();
       xhttp.onreadystatechange = function () {
         if (this.readyState == 4 && this.status == 200) {
           var data = JSON.parse(this.responseText);
           displayUserInfo(data);
         }
       };
       xhttp.open('GET', 'http://localhost:3000/comments', true);
       xhttp.send();
     });

function displayUserInfo(data) {
    var userInfoElement = document.getElementById('userInfo');
    userInfoElement.innerHTML = `Name: ${data.name}<br>Age: ${data.age}<br>City: ${data.city}`;
}

//var xhr = new XMLHttpRequest();
//
//// 2. Конфигурируем его: GET-запрос на URL 'phones.json'
//xhr.open('GET', 'http://localhost:3000/name', false);
//
//// 3. Отсылаем запрос
//xhr.send();
//
//// 4. Если код ответа сервера не 200, то это ошибка
//if (xhr.status != 200) {
//  // обработать ошибку
//  alert( xhr.status + ': ' + xhr.statusText ); // пример вывода: 404: Not Found
//} else {
//  // вывести результат
//  alert( xhr.responseText ); // responseText -- текст ответа.
//}


