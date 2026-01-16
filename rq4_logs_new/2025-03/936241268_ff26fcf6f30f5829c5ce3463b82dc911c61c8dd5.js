function debounce(func, delay) {
    let timer;
    return function (...args) {
        clearTimeout(timer);  // Clear previous timer
        timer = setTimeout(() => func(...args), delay);  // Set new timer
    };
}

// Simulating API call
function searchQuery(query) {
    console.log(`Searching for: ${query}`);
}

const debouncedSearch = debounce(searchQuery, 1000);

// Simulating user typing
debouncedSearch("Hello");
setTimeout(() => debouncedSearch("Hello, W"), 200);
setTimeout(() => debouncedSearch("Hello, World!"), 500);