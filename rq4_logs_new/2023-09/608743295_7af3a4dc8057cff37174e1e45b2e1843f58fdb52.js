function deleteButton(event) {
    // Show a confirmation dialog
    const confirmation = confirm('Are you sure you want to delete this activity?');

    if (confirmation) {
        const id =  event.target.getAttribute('registro');

        // Send a POST request to the server when the delete button is clicked
        fetch('/delete', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ id })
        })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok');
                }
                console.log('Activity deleted successfully');
                // Reload the page
                window.location.reload();
            })
            .catch(error => {
                console.error('There was a problem deleting the activity:', error);
            });
    }
};