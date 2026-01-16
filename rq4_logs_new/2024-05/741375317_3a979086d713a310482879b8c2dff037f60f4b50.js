
$('#content').summernote({
     tabsize: 2,
     height: 200,
     toolbar: [
          ['style', ['style']],
          ['font', ['bold', 'underline', 'clear']],
          ['color', ['color']],
          ['para', ['ul', 'ol', 'paragraph']],
          ['table', ['table']],
          ['insert', ['link', 'picture']],
          ['view', ['fullscreen']]
     ],
     callbacks: {
          onImageUpload: function (files) {
               // Lặp qua từng file được tải lên
               for (var i = 0; i < files.length; i++) {
                    var allowedTypes = ['jpg', 'jpeg', 'png']; // Danh sách các định dạng hình ảnh mà bạn muốn chấp nhận
                    var ext = files[i].name.split('.').pop().toLowerCase();
                    if (allowedTypes.indexOf(ext) === -1) {
                         alert('Chỉ chấp nhận các hình ảnh có định dạng JPG, JPEG, PNG');
                         return false; // Hủy việc tải lên nếu không đúng định dạng
                    }
               }

               var editor = $(this);
               for (var i = 0; i < files.length; i++) {
                    var file = files[i];
                    var reader = new FileReader();
                    reader.onload = function (e) {
                         editor.summernote('insertImage', e.target.result);
                    }
                    reader.readAsDataURL(file);
               }
          }
     }
});

