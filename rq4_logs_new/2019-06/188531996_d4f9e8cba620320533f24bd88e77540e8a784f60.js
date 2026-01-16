$(function() {

  var search_list = $("#user-search-result");
  var member_list = $(".chat-group-user__name");
  
  function appendUser(users) {
     var html = 
      `<div class="chat-group-user clearfix">
    <p class="chat-group-user__name">${ users.name }</p>
    <a class="user-search-add chat-group-user__btn chat-group-user__btn--add" data-user-id="${ users.id }" data-user-name=${ users.name }>追加</a>
  </div>`
     search_list.append(html);
     return html;
   }
  
  function appendMember(name, user_id) {
     var html = 
      `<div class='chat-group-user clearfix js-chat-member' id='chat-group-user-8'>
    <input name='group[user_ids][]' type='hidden' value=${ user_id }>
    <p class='chat-group-user__name'>${ name }</p>
    <a class='user-search-remove chat-group-user__btn chat-group-user__btn--remove js-remove-btn'>削除</a>
  </div>`
     member_list.append(html);
   }
  
   function appendNoUser(users) {
      var html = `<div class="chat-group-user clearfix">
    <p class="chat-group-user__name">${ users }</p>
    </div>`
      search_list.append(html);
    }
  
  $(function(){
    $("#user-search-field").on("keyup", function() {
      var input = $("#user-search-field").val();
  
      $.ajax({
        type: 'GET',
        url: '/users',
        data: { keyword: input },
        dataType: 'json'
      })
  
      .done(function(users) {
        $("#user-search-result").empty();
        if (users.length !== 0) {
          users.forEach(function(users){
            appendUser(users);
          });
        }
        else {
          appendNoUser("一致するユーザーが見つかりません");
        }
      })
      .fail(function() {
        alert('ユーザー検索に失敗しました');
      })
  
  
    $(function(){
    $(document).on('click', '.user-search-add', function() {
      var name = $(this).attr("data-user-name");
      var user_id = $(this).attr("data-user-id");
      $(this).parent().remove();
      appendMember(name, user_id);
    });
  
    $(document).on("click", '.user-search-remove', function() {
        $(this).parent().remove();
          });
    });
  });
  });
  });