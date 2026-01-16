from django.shortcuts import render
from django.conf import settings
from django.core.files.storage import FileSystemStorage

# Create your views here.
def index(request):
    return render(request, 'core/index.html', {})

def upload_image(request):
    FOLDER = 'photos/'

    if request.method == 'POST' and request.FILES['image']:
        # Get image from request
        image = request.FILES['image']
        # Instantiate FileSystemStorage
        fs = FileSystemStorage(location="photos/")
        filename = fs.save(myfile.name, image)
        # File url (folder+name)
        file_url = FOLDER + fs.url(filename)

        # Render page and pass path to inage.as context./
        return render(request, 'core/your_meme.html', {
            'uploaded_file_url': file_url
        })

    return render(request, 'core/index.html')