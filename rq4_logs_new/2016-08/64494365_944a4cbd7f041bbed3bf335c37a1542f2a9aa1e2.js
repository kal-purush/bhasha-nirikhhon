class UI{

    static togglePlaylists(){
        this.arePlaylistsVisible = !this.arePlaylistsVisible;
        this.inform();
    }

    static subscribe(onChange) {
        this.onChanges.push(onChange);
    }

    static inform() {
        this.onChanges.forEach(cb => cb());
    }
}

UI.arePlaylistsVisible = true;

UI.onChanges = [];
export default UI;