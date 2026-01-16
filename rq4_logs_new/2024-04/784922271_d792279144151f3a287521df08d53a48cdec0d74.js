$(document).ready(function () {
    $.ajax({
        url: '/Home/GetAllPhotos',
        type: 'GET',
        success: function (data) {
            displayPhotos(data);
        },
        error: function (xhr, status, error) {
            console.error('Erreur lors du chargement des photos : ' + error);
        }
    });
});

function filterPhotos(categoryId) {
    $.ajax({
        url: '/Home/GetPhotosByCategory',
        type: 'GET',
        data: { categoryId: categoryId },
        success: function (data) {
            displayPhotos(data);
        },
        error: function (xhr, status, error) {
            console.error('Erreur lors du chargement des photos : ' + error);
        }
    });
}

function showAllPhotos() {
    $.ajax({
        url: '/Home/GetPhotosByCategory',
        type: 'GET',
        data: { categoryId: 0 },
        success: function (data) {
            displayPhotos(data);
        },
        error: function (xhr, status, error) {
            console.error('Erreur lors du chargement des photos : ' + error);
        }
    });
}

function displayPhotos(photoPaths) {
    var photoGrid = $('#photoGrid');
    photoGrid.empty();
    $.each(photoPaths, function (index, imagePath) {
        var photoItem = $('<div class="photo-item"></div>');
        var img = $('<img src="' + imagePath + '" alt="Image" class="photo" />');
        var overlay = $('<div class="overlay"><div class="overlay-text">Cliquez pour agrandir</div></div>');
        photoItem.append(img);
        photoItem.append(overlay);
        photoGrid.append(photoItem);
    });
}

var categoryButtons = document.querySelectorAll('.category-button');
categoryButtons.forEach(function (button) {
    button.addEventListener('click', function () {
        categoryButtons.forEach(function (btn) {
            btn.classList.remove('selected');
        });
        this.classList.add('selected');
    });
});

$(document).ready(function () {
    $("#allButton").click();
});