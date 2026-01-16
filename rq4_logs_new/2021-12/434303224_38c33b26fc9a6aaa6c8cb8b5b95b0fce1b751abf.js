async function main() {
	// Load the JSON data
	const httpResponse = await fetch("dunkinDonuts.json");
	const dunkinDonuts = await httpResponse.json();

	// labels: ["1901", "1902", "1903", ..., "2019"]
	// data:   [12531235, 34563456, 5634563456, ..., 234623462]

	// Construct labels and data
	const labels = [];
	const data = [];
	let numberOfStore = [];

	for (const dunkins of dunkinDonuts) {
		if (labels.includes(dunkins.state)) {
		} else {
			labels.push(dunkins.state);
		}

		numberOfStore.push(dunkins.state);
	}

	labels.sort();
	numberOfStore.sort();
	console.log(numberOfStore);

	let stores = 0;

	for (let i = 0; i < numberOfStore.length; i++) {
		if (
			numberOfStore[i] !== numberOfStore[i + 1] ||
			numberOfStore[i + 1] === undefined
		) {
			stores++;
			data.push(stores);
			stores = 0;
		}

		if (numberOfStore[i] === numberOfStore[i + 1]) {
			stores++;
		}
	}

	console.log(data);
	labels.sort();
	// Draw the chart
	const ctx = document.getElementById("myChart").getContext("2d");
	const myChart = new Chart(ctx, {
		type: "bar",
		data: {
			labels: labels,
			datasets: [
				{
					data: data,
				},
			],
		},
	});
}

main();