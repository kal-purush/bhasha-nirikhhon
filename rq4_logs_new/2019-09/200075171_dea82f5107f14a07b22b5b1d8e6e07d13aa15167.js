
$(function(){

  var seach_list = $("#user-search-result")

  function appendUser(user){ 
    var html = `<div class="chat-group-user clearfix">
                  <p class="chat-group-user__name">${user.name}</p>
                  <a class="user-search-add chat-group-user__btn chat-group-user__btn--add" data-user-id="${user.id}" data-user-nickname="${user.name}">追加</a>
                </div>`
    seach_list.append(html);           
    }

  function appendErrMsgToHTML(user) {
    var html = `<div class='chat-group-user clearfix'>${ user }
                </div>`
    seach_list.append(html);            
  } 

  $("#user-search-field").on("keyup", function() {
    var input = $("#user-search-field").val();
    console.log(input);   

    $.ajax({
      type: "Get",
      url: "/users",
      data: { keyword: input },
      dataType: "json"
    })

    .done(function(users) {
      console.log(users)
      $("#user-search-result").empty();
      if (users.length !== 0) {
         users.forEach(function(user){
         appendUser(user);
        })
      }
      else {
        appendErrMsgToHTML("一致するユーザーはいません");
      }
    })
    .fail(function(){
      alert("検索に失敗しました");
    })
  })
});