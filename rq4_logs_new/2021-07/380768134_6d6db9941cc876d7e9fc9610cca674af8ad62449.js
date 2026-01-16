const drop = () => {
    const fileInputs = document.querySelectorAll('[name="upload"]');

    ['dragenter', 'dragleave', 'dragover', 'drop'].forEach(evtName => {
        fileInputs.forEach(input => {
            input.addEventListener(evtName, preventDefaults, false);
        });
    });

    function preventDefaults(evt) {
        evt.preventDefault();
        evt.stopPropagation();
    }

    function highlight(item) {
        item.closest('.file_upload').style.border = "5px solid yellow";
        item.closest('.file_upload').style.backgroundColor = "rgba(0,0,0,0.7)";
    }

    function unHighlight(item) {
        item.closest('.file_upload').style.border = "none";

        if (item.closest('.calc_form')) {
            item.closest('.file_upload').style.backgroundColor = "#ffffff";
        } else {
            item.closest('.file_upload').style.backgroundColor = "#ededed";
        }
    }

    ['dragenter', 'dragover'].forEach(evtName => {
        fileInputs.forEach(input => {
            input.addEventListener(evtName, () => highlight(input), false);
        });
    });

    ['dragleave', 'drop'].forEach(evtName => {
        fileInputs.forEach(input => {
            input.addEventListener(evtName, () => unHighlight(input), false);
        });
    });

    fileInputs.forEach(input => {
        input.addEventListener('drop', (evt) => {
            input.files = evt.dataTransfer.files;

            let dots; // содержит ... если имя файла больше 6 символов

            // 'abcdefjh.png' => ['abcdefjh', 'png']
            const splitArr = input.files[0].name.split('.');

            splitArr[0].length > 6 ? dots = '...' : dots = '.';

            const name = splitArr[0].substring(0, 6) + dots + splitArr[1];

            input.previousElementSibling.textContent = name;
        });
    });
};

export default drop;