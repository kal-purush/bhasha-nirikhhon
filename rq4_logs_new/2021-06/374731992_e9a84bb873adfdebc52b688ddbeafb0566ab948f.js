function calculate_width_scroll() {
    const table_elem = document.querySelectorAll('.table_elem_form');
    table_elem.forEach(element => {
        if (element.scrollHeight > element.clientHeight) {
            const width_scroll = element.offsetWidth - element.clientWidth - 3;
            const headers = element.previousElementSibling;
            headers.children[headers.children.length - 1].style.width = headers.children[headers.children.length - 1].clientWidth + width_scroll + "px";
        }
    });
}