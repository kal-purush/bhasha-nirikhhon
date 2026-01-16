import Backbone from 'backbone';

import WordView from 'views/word_view';

var ApplicationView = Backbone.View.extend({
  initialize: function() {
    this.wordListElement = this.$('#word-list tbody');
    this.newWordInput = this.$('#new-word-form input[name=word]');

    // Maintain a list of views, to cut down
    // on re-rendering time
    this.wordViews = [];
    this.model.each((word) => {
      this.addWord(word);
    });

    this.listenTo(this.model, 'add', this.addWord);
    this.listenTo(this.model, 'update', this.render);
  },

  render: function() {
    this.wordListElement.html('');

    this.wordViews.forEach((wordView) => {
      this.wordListElement.append(wordView.$el);
    });
  },

  events: {
    'submit #new-word-form': 'playWord'
  },

  playWord: function(event) {
    event.preventDefault();
    console.log("In playWord");

    var text = this.newWordInput.val();
    this.newWordInput.val('');

    if (text) {
      this.model.add({text: text});
    }
  },

  addWord: function(word) {
    var wordView = new WordView({
      model: word
    });
    this.wordViews.push(wordView);
  }

});

export default ApplicationView;