if (!!document.querySelector(".DailySchedule-Nav")) {
    document.querySelector(".DailySchedule-Nav").insertAdjacentHTML("afterend", '<div style="display: flex; justify-content: center;"><button class="Navigation-Button completedWinnings" style="border: 1px solid white;">Completed Winnings: -</button><button class="Navigation-Button expectedWinnings" style="border: 1px solid white;">Expected Winnings: -</button><button class="Navigation-Button totalWinnings" style="border: 1px solid white;">Total Winnings: -</button></div>');
    let winInterval = window.setInterval(() => {
        if (!!document.querySelector(".completedWinnings")) {
            const netWinnings = (() => {
                const blurbs = Array.prototype.slice.call(document.querySelectorAll(".GameWidget-Outcome-Blurb")).filter(e => e.innerText.indexOf("You bet") === 0);
                if (!blurbs.length) {
                    return null;
                } else {
                    return blurbs.map(e => {
                        const words = e.innerText.split(/\s/);
                        const bet = parseInt(words[3]);
                        if (words[words.length - 1] == "lost.") {
                            return -bet;
                        } else {
                            const winnings = words[words.length - 1];
                            return parseInt(winnings.substring(0, winnings.length - 1)) - bet;
                        }
                    }).reduce((a, b) => a + b, 0);
                }
            })();
            const expectedWinnings = (() => {
                const finalWidgets = Array.prototype.slice.call(document.querySelectorAll(".GameWidget-Header-Wrapper")).filter(e => e.innerText.indexOf("FINAL") !== 0);
                if (!finalWidgets.length) {
                    return null;
                } else {
                    return finalWidgets.map(game => {
                        const [away, home] = Array.prototype.slice.call(game.querySelectorAll(".GameWidget-ScoreLine"));
                        const awayTexts = away.innerText.split(/\s/);
                        const homeTexts = home.innerText.split(/\s/);
                        const betOnAway = awayTexts.length == 8;
                        const betOnHome = homeTexts.length == 8;
                        const awayScore = parseFloat(awayTexts[awayTexts.length-2]);
                        const homeScore = parseFloat(homeTexts[homeTexts.length-2]);
                        if ((betOnAway || betOnHome) && !(awayScore === homeScore)) {
                            const betText = betOnAway ? awayTexts : homeTexts;
                            const betAmt = parseInt(betText[betText.length - 4]);
                            const winAmt = parseInt(betText[betText.length - 3]);
                            const areYouWinning = (betOnAway && awayScore > homeScore) || (betOnHome && homeScore > awayScore);
                            return areYouWinning ? (winAmt - betAmt) : -betAmt;
                        } else {
                            return 0;
                        }
                    }).reduce((a, b) => a + b, 0);
                }
            })();
            if (netWinnings !== null) {
                document.querySelector(".completedWinnings").innerText = `Completed Winnings: ${netWinnings}`;
            } else {
                document.querySelector(".completedWinnings").innerText = "Completed Winnings: N/A";
            }
            if (expectedWinnings !== null) {
                document.querySelector(".expectedWinnings").innerText = `Expected Winnings: ${expectedWinnings}`;
            } else {
                document.querySelector(".expectedWinnings").innerText = "Expected Winnings: N/A";
            }
            if (netWinnings !== null || expectedWinnings !== null) {
                document.querySelector(".totalWinnings").innerText = `Total Winnings: ${(netWinnings !== null ? netWinnings : 0) + (expectedWinnings !== null ? expectedWinnings : 0)}`;
            } else {
                document.querySelector(".totalWinnings").innerText = "Total Winnings: N/A";
            }
        }
    }, 1000);
} else {
    console.log("Blaseball Winnings script not loaded, can't find navbar");
}