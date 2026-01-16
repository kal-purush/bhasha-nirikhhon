$(document).on('turbolinks:load', ()=> {
  $(function(){
    function buildHTML(comment){
      let html = `<div class="comment__list__user--name">
                  ${comment.user_nickname}
                  </div>
                  <div class="comment__list__content">
                    <div class="comment__list__content--body">
                    ${comment.comment}
                    </div>
                    <div class="comment__list__content--time">
                    ${comment.created_at}<br>
                    </div>
                  </div>`
      return html;
    }
    $('#new_comment').on('submit',function(e){
      e.preventDefault();
      let formData = new FormData(this);
      let url = $(this).attr('action');
      $.ajax({
        url: url,
        type: "POST",
        data:formData,
        dataType: "json",
        processData: false,
        contentType: false
      })
      .done(function(data){
        let html = buildHTML(data);
        $('.comment__list').append(html);
        $('.textbox').val('');
        $('.comment__submit__btn').prop("disabled", false);
        $('.comment__list').animate({scrollTop: $(".comment__list")[0].scrollHeight});
      })
      .fail(function() {
        alert("メッセージ送信に失敗しました");
      });
    })
  })
})