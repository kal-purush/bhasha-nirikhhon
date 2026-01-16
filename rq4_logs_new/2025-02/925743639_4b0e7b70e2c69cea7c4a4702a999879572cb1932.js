function toggleVisibility(element) {
    const table = element.nextElementSibling;
    if (table.style.display === "none" || !table.style.display) {
        table.style.display = "table";
    } else {
        table.style.display = "none";
    }
}