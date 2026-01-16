async function calculateScore(id) {
    try {
        const response = await fetch(`/vas-dev/indicators/calculate/${id}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            }
        });

        if (!response.ok) {
            throw new Error(`Error: ${response.status} ${response.statusText}`);
        }

        const data = await response.json();

        if (data.success) {
            console.log(data.message);
        } else {
            console.error(data.message);
        }
    } catch (error) {
        console.error('Fetch error:', error.message);
        document.getElementById("result").innerText = 'An error occurred while calculating scores.';
    }
}