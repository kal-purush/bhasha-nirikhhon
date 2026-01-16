
      var userData= document.getElementsByTagName("LI");
      for (var i = 0; i < userData.length; i++) {
        var span = document.createElement("SPAN");
        var txt = document.createTextNode("\u00D7");
        span.className = "delete";
        span.appendChild(txt);
        userData[i].appendChild(span);
      }
      
      
      var hide = document.getElementsByClassName("delete");
      for (var i = 0; i < hide.length; i++) {
        hide[i].onclick = function() {
          var div = this.parentElement;
          div.style.display = "none";
        }
      }
      
      
      var list = document.querySelector('ol');
      list.addEventListener('click', function(sha) {
        if (sha.target.tagName === 'LI') {
          sha.target.classList.toggle('done');
        }
      }, false);
      
      function myList() {
        var li = document.createElement("li");
        var inputValue = document.getElementById("myInput").value;
        var t = document.createTextNode(inputValue);
        li.appendChild(t);
        if (inputValue === '') {
          document.getElementById('empty').innerHTML=("**Please enter your activity**");
        } else {
          document.getElementById('empty').style.display='none';
          document.getElementById("addList").appendChild(li);
        }
        document.getElementById("myInput").value = "";
      
        var span = document.createElement("SPAN");
        var txt = document.createTextNode("\u00D7");
        span.className = "delete";
        span.appendChild(txt);
        li.appendChild(span);
      
        for (var i = 0; i < hide.length; i++) {
          hide[i].onclick = function() {
            var div = this.parentElement;
            div.style.display = "none";
          }
        }
      }