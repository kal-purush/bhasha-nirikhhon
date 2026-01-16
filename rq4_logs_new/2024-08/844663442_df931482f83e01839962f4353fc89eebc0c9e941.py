from django import forms

from MusicApp.common.session_decorator import session_decorator
from MusicApp.musics.models import Album, Song
from MusicApp.settings import session


class AlbumBaseForm(forms.Form):
    album_name = forms.CharField(label="Album Name", max_length=30, required=True)
    image_url = forms.URLField(label="Image URL", required=True)
    price = forms.DecimalField(label="Price", min_value=0.0, required=True)


class AlbumCreateForm(AlbumBaseForm):
    @session_decorator(session)
    def save(self):
        new_album = Album(
            album_name=self.cleaned_data["album_name"],
            image_url=self.cleaned_data["image_url"],
            price=self.cleaned_data["price"],
        )

        session.add(new_album)


class AlbumUpdateForm(AlbumBaseForm):
    def save(self, album):
        album.album_name = self.cleaned_data["album_name"]
        album.image_url = self.cleaned_data["image_url"]
        album.price = self.cleaned_data["price"]


class AlbumDeleteForm(AlbumBaseForm):
    pass


class SongBaseForm(forms.Form):
    song_name = forms.CharField(
        label="Song Name",
        max_length=20,
        required=True
    )

    album = forms.ChoiceField(
        label="Album",
        choices=[],  # we overwrite that in the init
    )

    @session_decorator(session)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        album_choices = [(album.id, album.album_name) for album in session.query(Album).all()]
        self.fields["album"].choices = album_choices


class SongCreateForm(SongBaseForm):
    @session_decorator(session)
    def save(self):
        new_song = Song(
            song_name=self.cleaned_data["song_name"],
            album_id=self.cleaned_data["album"],
        )

        session.add(new_song)