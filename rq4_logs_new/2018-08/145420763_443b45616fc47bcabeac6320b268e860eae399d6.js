$('#editPostModal').on('show.bs.modal', function (event) {
    console.log('editPost show modal');
    var button = $(event.relatedTarget) // Button that triggered the modal
    var post = button.data('post') // Extract info from data-* attributes
     // If necessary, you could initiate an AJAX request here (and then do the updating in a callback).
    // Update the modal's content. We'll use jQuery here, but you could use a data binding library or other methods instead.
    var modal = $(this)
    // modal.find('.modal-title').text('New message to ' + recipient)
    modal.find('.modal-body #postName').val(post.name);
     modal.find('.modal-body #postId').val(post.id);
  })

  $('#deletePostModal').on('show.bs.modal', function (event) {
    console.log('deletePost show modal');
    var button = $(event.relatedTarget) // Button that triggered the modal
    var post = button.data('post') // Extract info from data-* attributes
     // If necessary, you could initiate an AJAX request here (and then do the updating in a callback).
    // Update the modal's content. We'll use jQuery here, but you could use a data binding library or other methods instead.
    var modal = $(this)
    // modal.find('.modal-title').text('New message to ' + recipient)
 
    modal.find('.modal-body #postId').val(post.id);
  })