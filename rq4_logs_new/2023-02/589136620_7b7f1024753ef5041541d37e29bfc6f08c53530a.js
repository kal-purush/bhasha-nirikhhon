let image="",file="",youtube="", imageCount=0,fileCount=0,temp="", youtubeCount=0;

$(document).ready(function(){

    const images = document.querySelector('#new_image');
    const files = document.querySelector('#new_file');
    const editImages = document.querySelector('#edit_new_image');
    const editFiles = document.querySelector('#edit_new_file');

    if(images)
    {
        // Listen for files selection
        images.addEventListener('change', (e) => {
            // Retrieve all files
            const uploadedImages = images.files;
            $('#imageCountError').text('');
            $('#imageError').text('');

            // Check files count
            if (uploadedImages.length > 5  ) {
                $('#imageCountError').text('الحد الأقصى للصور المرفقة 5');
            }
        });
    }
    if(editImages)
    {
        // Listen for files selection
        editImages.addEventListener('change', (e) => {
            // Retrieve all files
            const newImages = editImages.files;
            console.log('new'+newImages);
            console.log('old'+imageCount);
            $('#editImageCountError').text('');
            // Check files count
            if ((newImages.length+imageCount) > 5  ) {
                $('#editImageCountError').text('الحد الأقصى للصور المرفقة 5');
            }
        });
    }

    if(files)
    {
        // Listen for files selection
        files.addEventListener('change', (e) => {
            // Retrieve all files
            const uploadedFiles = files.files;
            $('#fileCountError').text('');
            // Check files count
            if (uploadedFiles.length > 2) {
                $('#fileCountError').text('الحد الأقصى للملفات المرفقة 2');
            }
        });
    }
    if(editFiles)
    {
        // Listen for files selection
        editFiles.addEventListener('change', (e) => {
            // Retrieve all files
            const newFiles = editFiles.files;
            $('#editFileCountError').text('');
            // Check files count
            if (newFiles.length + fileCount > 2) {
                $('#editFileCountError').text('الحد الأقصى للملفات المرفقة 2');
            }
        });
    }
    $('#allow_comments').on('click',function () {
        var checkBox = document.getElementById('allow_comments');
        if(checkBox.checked === true)
        {
            $('#allow_comments').val("1")
            $('#allow_comments').attr('checked','checked')
        }
        else
        {
            $('#allow_comments').val("0")
            $('#allow_comments').removeAttr('checked')
        }
    })

    var youtubeCounter = 1,editYoutubeCounter = 1;

    addYoutube();

    $('#add-youtube').on('click', addYoutube);
    $('#remove-youtube').on('click', removeYoutube);
    $('#add-new-youtube').on('click', addNewYoutube);
    $('#remove-new-youtube').on('click', removeNewYoutube);

    function addYoutube()
    {
        var youtube = "<div class='col-12'> <input class='form-control my-1' type='text' name='new_youtube[]' id='new_youtube_" + youtubeCounter + "'>" +
            "<div class='text-danger' id='youtubeError"+ (youtubeCounter-1) + "'></div></div>"

        $('#youTubes').append(youtube)
        youtubeCounter ++;
    }

    function removeYoutube()
    {
        if (youtubeCounter > 2)
        {
            youtubeCounter --;
            $('#new_youtube_' + youtubeCounter).remove();
            $('#youtube-description-' + youtubeCounter).remove();
            $('#youtubeError' + (youtubeCounter-1)).remove();
            $('#youtubeDescriptionError' + (youtubeCounter-1)).remove();
        }
    }
    function addNewYoutube()
    {
        var youtube = "<div class='col-12'> <input class='form-control my-1' type='text' name='new_youtube[]' id='edit_new_youtube_" + editYoutubeCounter + "'>" +
            "<div class='text-danger' id='editYoutubeError"+ (editYoutubeCounter-1) + "'></div></div>"

        $('#editYouTubes').append(youtube)
        editYoutubeCounter ++;
    }

    function removeNewYoutube()
    {
        if (editYoutubeCounter > 2)
        {
            editYoutubeCounter --;
            $('#edit_new_youtube_' + editYoutubeCounter).remove();
            $('#editYoutubeError' + (editYoutubeCounter-1)).remove();
        }
    }
    /***
     * ***************************************************************************************************************
     */
    $.ajaxSetup({
        headers: {
            'X-CSRF-TOKEN': $('meta[name="csrf-token"]').attr('content')
        }
    });
    $('select[name="classification_id"]').on('change',function (e) {
        if($(this).val() === '-1')
            $('#classification_name').show();
        else
            $('#classification_name').hide();
    })
    $('select[name="classification_id"]').on('change',function (e) {
        if($(this).val() === '-1')
            $('#edit_classification_name').show();
        else
            $('#edit_classification_name').hide();
    })
    $('body').on('click', '.create-btn', function (e)
    {
        e.preventDefault();
        $('#achievementForm').find('input').val('');
        $('.text-danger').text('');
        for (let i=2; i<youtubeCounter ; i++)
        {
            $('#new_youtube_' + i).remove();
            $('#youtube-description-' + i).remove();
        }
    });
    /************************** store article **********************/
    $('#initiativesForm').ajaxForm({
        success:function(response)
        {
            if(response)
            {
                Swal.fire({
                    title: 'تمت الإضافة بنجاح',
                    text: "",
                    icon: 'success',
                    showCancelButton: false,
                    confirmButtonText: "موافق",
                    confirmButtonColor: "#109f99",
                    closeOnConfirm: true,
                    closeOnCancel: true
                }).then((result) => {
                    if (result.isConfirmed) {
                        $('#add-initiative-eModal').hide();
                        $('#add-initiative-eModal').trigger('reset');
                        window.location.reload();
                    }
                });
            }
        },
        error: function (response) {
            $('#titleError').text('');
            $('#targetGroupError').text('');
            $('#teamError').text('');
            $('#classificationIdError').text('');
            $('#nameError').text('');
            $('#descriptionError').text('');
            $('#startDateError').text('');
            $('#endDateError').text('');
            $('#imageError').text('');
            $('#fileError').text('');

            for (var i=0; i<youtubeCounter ; i++)
            {
                $('#youtubeError'+i).text('')
            }
            $('#titleError').text(response.responseJSON.errors.title);
            $('#targetGroupError').text(response.responseJSON.errors.target_group);
            $('#teamError').text(response.responseJSON.errors.team);
            $('#classificationIdError').text(response.responseJSON.errors.classification_id);
            $('#nameError').text(response.responseJSON.errors.name);
            $('#descriptionError').text(response.responseJSON.errors.description);
            $('#startDateError').text(response.responseJSON.errors.start_date);
            $('#endDateError').text(response.responseJSON.errors.end_date);
            $('#imageError').text(response.responseJSON.errors['new_image.0']);
            $('#fileError').text(response.responseJSON.errors['new_file.0']);

            for (var i=0; i<youtubeCounter ; i++)
            {
                if(response.responseJSON.errors['new_youtube.'+i] != null)
                    $('#youtubeError'+i).text(response.responseJSON.errors['new_youtube.'+i])
            }
        }
    });

    /************************** Update article **********************/
    $('body').on('click', '.edit-btn', function (e)
    {
        e.preventDefault();
        youtubeCount=0;
        var content_id = $(this).data('id');
        var action = 'initiatives/update/'+content_id;
        $('#editInitiativeForm').attr('action',action);

        $.get('initiatives/edit/' + content_id , function (data)
        {
            imageCount=data.count;
            fileCount=data.files;
            youtube="";image="";file="";
            $('#id').val(data.data.id);
            $('#edit_title').val(data.data.title);
            $('#edit_target_group').val(data.data.initiative.target_group);
            $('#edit_team').val(data.data.initiative.team);
            $('#edit_description').val(data.data.initiative.description);
            $('#edit_start_date').val(data.data.initiative.start_date);
            $('#edit_end_date').val(data.data.initiative.end_date);
            $('#option'+data.data.classification_id).attr('selected','selected');

            if(data.data.allow_comments === 1 )
                $('#allow_comments').attr('checked','checked')
            else
                $('#allow_comments').removeAttr('checked')

            for(let i =0 ; i<data.data.content_file.length ; i++)
            {
                if(data.data.content_file[i].mime === 'image')
                {
                    image +=
                        `<div class="image w-auto" id="image${data.data.content_file[i].id}">
                            <img src="${data.data.content_file[i].AttPath}" alt="image" style="height:100px!important;width:100px!important;  " width="100px" height="100px" class="img-thumbnail img-responsive h-75">
                        ${(imageCount>1) ? `<a href="#" class="DeleteImage" data-mime="image" onclick="deleteFun(${data.data.content_file[i].id});" type="button" id="remove${data.data.content_file[i].id}">&times;</a>` : ""}
                        </div>`
                }
                else if(data.data.content_file[i].mime === 'file')
                {
                    file+=
                        `<div class="file card d-flex flex-row w-auto mx-2 justify-content-between my-2" id="file${data.data.content_file[i].id}">
                            <a href="${data.data.content_file[i].AttPath}" class="ms-2" target="_blank" > ${data.data.content_file[i].name}</a>
                            <a href="#" data-mime="file" onclick="deleteFun(${data.data.content_file[i].id});" type="button" id="remove${data.data.content_file[i].id}">&times;</a></div>`;
                }
                else if(data.data.content_file[i].mime === 'youtube')
                {
                    youtubeCount++;
                    youtube+= `<div class="col-12 d-flex" id="youtube${data.data.content_file[i].id}"> <input class="form-control my-1" type="text" name="old_youtube[${youtubeCount}]" id="old_youtube[${youtubeCount}]" value="${data.data.content_file[i].name}">
                                <a href="#" data-mime="youtube" class="align-self-center size-24 mx-2" onclick="deleteFun(${data.data.content_file[i].id});" type="button" id="remove${data.data.content_file[i].id}">&times;</a></div>
                                <div class="text-danger col-12" id="youtubeError${youtubeCount}"></div>`
                }
            }
            $('#prev_images').empty()
            $('#prev_files').empty()
            $('#prev_youTubes').empty()
            $('#prev_images').append(image)
            $('#prev_files').append(file)
            $('#prev_youTubes').append(youtube)
        });
    });
    $('#editInitiativeForm').ajaxForm({
        success:function(response)
        {
            if(response)
            {
                Swal.fire({
                    title: 'تم التعديل بنجاح',
                    text: "",
                    icon: 'success',
                    showCancelButton: false,
                    confirmButtonText: "موافق",
                    confirmButtonColor: "#109f99",
                    closeOnConfirm: true,
                    closeOnCancel: true
                }).then((result) => {
                    if (result.isConfirmed) {
                        $('#edit-initiative-eModal').hide();
                        $('#edit-initiative-eModal').trigger('reset');
                        window.location.reload();
                    }
                });
            }
        },
        error: function (response)
        {
            $('#editTitleError').text('');
            $('#editTargetGroupError').text('');
            $('#editTeamError').text('');
            $('#editDescriptionError').text('');
            $('#editStartDateError').text('');
            $('#editEndDateError').text('');
            $('#editClassificationIdError').text('');
            $('#editNameError').text('');
            $('#editImageError').text('');
            $('#editFileError').text('');

            for (var i=0; i<editYoutubeCounter ; i++)
            {
                $('#editYoutubeError'+i).text('');
            }
            for (var i=1; i<=youtubeCount ; i++)
            {
                $('#youtubeError'+i).text('');
            }
            $('#editTitleError').text(response.responseJSON.errors.title);
            $('#editTargetGroupError').text(response.responseJSON.errors.target_group);
            $('#editTeamError').text(response.responseJSON.errors.team);
            $('#editDescriptionError').text(response.responseJSON.errors.description);
            $('#editStartDateError').text(response.responseJSON.errors.start_date);
            $('#editEndDateError').text(response.responseJSON.errors.end_date);
            $('#editClassificationIdError').text(response.responseJSON.errors.classification_id);
            $('#editNameError').text(response.responseJSON.errors.name);
            $('#editImageError').text(response.responseJSON.errors['new_image.0']);
            $('#editFileError').text(response.responseJSON.errors['new_file.0']);

            for (var i=0; i<editYoutubeCounter ; i++)
            {
                if(response.responseJSON.errors['new_youtube.'+i] != null)
                    $('#editYoutubeError'+i).text(response.responseJSON.errors['new_youtube.'+i])
            }
            for (var i=1; i<=youtubeCount ; i++)
            {
                if(response.responseJSON.errors['old_youtube.'+i] != null)
                    $('#youtubeError'+i).text(response.responseJSON.errors['old_youtube.'+i])
            }
        }
    });
    /************************* change status *******************************/

    $('.user-article-status').change(function() {
        var status = $(this).prop('checked') === true ? 'مسودة' : 'منشور';
        var content_id = $(this).data('id');
        const element = document.querySelector("#status-tag"+content_id);

        if(status === 'منشور'){
            element.style.background = "#ffc107";
            element.style.color = "rgb(70, 55, 55)";
            element.innerHTML = status;
        }
        if(status === 'مسودة'){
            element.style.background = "#ce3d4ee6";
            element.innerHTML = status;
        }

        $.ajax({
            type: "GET",
            dataType: "application/json",
            url: 'initiatives/status/'+status+'/'+content_id,
        });
    });

});
function deleteFun(id)
{
    let mime = $('#remove'+id).data("mime");
    $.ajax({
        type: "GET",
        dataType: "application/json",
        url: 'achievements/deleteFromDB/'+id+'/initiatives',
    });
    if(mime === 'image')
    {
        imageCount--;
        $('#image'+id).remove();
        if(imageCount === 1)
            $('.DeleteImage').remove();
        $('#editImageCountError').text('');
    }
    else if(mime === 'file')
    {
        fileCount--
        $('#file'+id).remove();
        $('#editFileCountError').text('');
    }
    else if(mime === 'youtube')
    {
        $('#youtube'+id).remove();
        $('#youtubeError'+youtubeCount).remove();
    }
}
