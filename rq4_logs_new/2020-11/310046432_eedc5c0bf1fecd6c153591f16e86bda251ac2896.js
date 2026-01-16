function getSearchValue() {

    document.getElementById('generate-button').onclick = function nice() {
        var repoUrl = document.getElementById('repoUrl').value;

        return repoUrl;
    }


}