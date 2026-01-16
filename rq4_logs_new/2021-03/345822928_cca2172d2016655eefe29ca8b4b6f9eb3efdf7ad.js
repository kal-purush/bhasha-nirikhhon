import consumer from "./consumer"

$(document).on('turbolinks:load', function(){
  if (gon.question_id){
    consumer.subscriptions.create({ channel: "CommentsChannel", question_id: gon.question_id }, {
      connected() {
        this.perform('follow')
      },

      received(data) {
        let template = require('./templates/comment.hbs')
        let comment = data.comment
        let klass = '.' + comment.commentable_type.toLowerCase() + '-comments-' + comment.commentable_id
        
        $(klass).append(template(data))
      }
    })
  }else{
    return undefined;
  }
})