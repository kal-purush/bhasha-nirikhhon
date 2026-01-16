// delete single user
$(document).ready(() => {
  $('#delete').on('shown.bs.modal', (event) => {
    var button = $(event.relatedTarget);
    var id = button.data('id');
    $('#agree-delete').on('click', function () {
      $.ajax({
        type: "DELETE",
        cache: false,
        url: $(this).data('url'),
        headers: { 'X-CSRF-TOKEN': $('meta[name="csrf-token"').attr('content') },
        data: { id: id },
        success: function (data, status) {
          if (status == 'success') {
            $('button[data-id = ' + data.id + ']').parents('tr').fadeOut();
            showAlert(data.flash_level, data.flash_message);
          }
        }
      });
    })
  })
})

// multiple-delete users
$(document).ready(() => {
  $('#multipleDelete').on('shown.bs.modal', (e) => {
    var ids = [];
    $('.checkbox:checked').each(function () {
      ids.push($(this).data('id'));
    })

    if (ids.length > 0) {
      $('#agree-multiple-delete').on('click', function () {
        $.ajax({
          type: "DELETE",
          cache: false,
          url: $(this).data('url'),
          headers: { 'X-CSRF-TOKEN': $('meta[name="csrf-token"').attr('content') },
          data: { ids: ids },
          success: function (data, status) {
            if (status == 'success') {
              $('.checkbox:checked').each(function () {
                $(this).parents('tr').fadeOut();
              })
              showAlert(data.flash_level, data.flash_message);
            }
          }
        });
      })
    }
  })
})

function showAlert($class, $message) {
  var alert = '';
  alert += '<div class="alert ' + $class + ' alert-dismissible fade show mb-3" role = "alert" >';
  alert += '<p class="m-0">' + $message + '</p>';
  alert += '<button type="button" class="close" data-dismiss="alert" aria-label="Close">';
  alert += '<span aria-hidden="true">×</span>';
  alert += '</button>';
  alert += '</div>';
  $('.breadcrumb-holder').after(alert);
  $('.delay').delay(2000).fadeOut(2000);
}

