 $(document).ready(function () {

        $('#newUser').submit(function (e) {
            var newUrl = Routing.generate('new_user');
            e.preventDefault();

            $.ajax({
                type: $(this).attr('method'),
                url: newUrl,
                data: $(this).serialize(),
                success: function (response) {
                    location.reload();
                    console.log("editado");
                },
                error: function (response) {
                    console.log("no editado");
                }
            });
        });

        $('.deleteUser').click(function (e) {

            var getUrl = Routing.generate('delete_user', {'id': $(this).attr('id')});
            e.preventDefault();

            $.ajax({
                type: 'delete',
                url:  getUrl,
                data: $(this).serialize(),
                success: function (response) {
                    location.reload();
                    console.log("borrado");
                },
                error: function (response) {
                    console.log("no borrado");
                }
            });
        });
 });