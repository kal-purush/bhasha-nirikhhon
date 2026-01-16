jQuery(document).ready(function () {

  $ = jQuery;
  
  var scope = $('#edit-author');

  // TODO: manage add_button event click

  $('#edit-creator-en, #edit-creator-sub, #edit-creator-email').hide();

  // Reformat author fields
  var container = $('<div class="author-container"></div>');

  var author_en_group = $('#edit-creator-en .form-type-textfield');
  var author_sub_group = $('#edit-creator-sub .form-type-textfield');
  var author_email_group = $('#edit-creator-email .form-type-textfield');

  var num_author = author_en_group.length;

  var has_author_sub_group = author_sub_group.length

  var author_item;
  for (var i = 0; i < num_author; i++) {
    author_item = $('<div class="author-item"></div>');

    if (has_author_sub_group) {
      author_sub = author_sub_group.eq(i).show();
      author_sub.find('label').hide();
      author_sub.find('input').attr('placeholder', 'Full name (thai)');
      author_item.append(author_sub);
    }

    author_en = author_en_group.eq(i).show();
    author_en.find('label').hide();
    author_en.find('input').attr('placeholder', 'Full name (eng.)');
    author_item.append(author_en);

    author_email = author_email_group.eq(i).show()
    author_email.find('label').hide();
    author_email.find('input').attr('placeholder', 'Email');
    author_item.append(author_email);

    if ((has_author_sub_group && author_sub.find('input').val()) || author_en.find('input').val() || author_email.find('input').val()) {
      author_item.addClass('visible');
    }

    container.append(author_item);
  };



  scope.append(container);

  var add_button = $('<button class="add-author-group add-multi-textfield">Add co-author</button>');
  add_button.click(function (e) {
    e.preventDefault();
    $('.author-item:hidden:first').show();
    if (!$('.author-item:hidden').length) {
      $(this).hide();
      container.append('<div class="description">Maximum to added are ' + $('.author-item').length + ' items.</div>');
    }
  });
  scope.append(add_button);

  $('.author-item').eq(0).find('label').eq(0).show().find('.label-text').html('Authors');
  
});