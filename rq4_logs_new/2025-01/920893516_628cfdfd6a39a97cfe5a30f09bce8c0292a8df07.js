let addBottleButton = undefined;
let feedDiv = undefined;
let feeds = [];
const Days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thur', 'Fri', 'Sat']; // Adjusted index to align with `getUTCDay`

window.onload = function () {
    addBottleButton = document.getElementById('add-bottle');
    feedDiv = document.getElementById('previous-feeds');
    const dialog = document.querySelector("dialog");
    const closeButton = document.querySelector("dialog button");
    const feedInput = document.getElementById('add-feed');
    RemoveOverOneWeek();

    GenerateFeedList();

    addBottleButton?.addEventListener('click', (e) => {
        e.preventDefault();
        dialog.showModal();
    });

    closeButton.addEventListener("click", () => {
        let currentFeed = {
            Time: new Date(),
            Value: parseFloat(feedInput.value) || 0 // Ensure Value is a number
        };
        
        if (currentFeed.Value > 0) {
            let items = JSON.parse(localStorage.getItem('FeedList')) ?? [];
            items.push(currentFeed);
            feeds.push(currentFeed);
            localStorage.setItem('FeedList', JSON.stringify(items));
            feedInput.value = 0;
        }
       
        dialog.close();
        GenerateFeedList();
    });
};

function GenerateFeedList() {
    if (!feeds.length) {
        feedDiv.innerText = "No Data To Display! Add A Feed";
    } else {
        feedDiv.innerText = '';
        const FeedList = document.createElement('ul');
        const dailyTotals = {}; // Object to store daily totals

        feeds.forEach(feed => {
            let date = new Date(feed.Time);
            let li = document.createElement('li');
            li.innerHTML = `<span>${Days[date.getUTCDay()]} ${
                (date.getHours() % 12 || 12)
              }:${date.getMinutes().toString().padStart(2, '0')} ${
                date.getHours() >= 12 ? 'PM' : 'AM'
              }</span> <span>Drank: ${feed.Value} oz</span>`;
            FeedList.appendChild(li);

            // Calculate totals for each day
            let dayName = Days[date.getUTCDay()];
            if (!dailyTotals[dayName]) {
                dailyTotals[dayName] = 0;
            }
            dailyTotals[dayName] += Number(feed.Value);
        });
        feedDiv.appendChild(FeedList);

        // Append daily totals at the bottom
        const TotalsDiv = document.createElement('div');
        TotalsDiv.style.marginTop = '20px'; // Add some spacing for clarity
        TotalsDiv.innerHTML = '<h3>Daily Totals</h3>';
        const TotalsList = document.createElement('ul');
        for (let day in dailyTotals) {
            let totalLi = document.createElement('li');
            totalLi.textContent = `${day}: ${dailyTotals[day]} oz`;
            TotalsList.appendChild(totalLi);
        }
        TotalsDiv.appendChild(TotalsList);
        feedDiv.appendChild(TotalsDiv);
    }
}

function RemoveOverOneWeek() {
    const List = localStorage.getItem('FeedList');
    const items = List ? JSON.parse(List) : [];
    feeds = items;

    const oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

    if (items.length > 0) {
        const inDateItems = items.filter(item => new Date(item.Time) >= oneWeekAgo);
        feeds = inDateItems; // Update feeds with filtered data
        localStorage.setItem('FeedList', JSON.stringify(inDateItems));
    }
}