var app = new Vue({
    el: ".susano---landing-page------",
    data: {
        discord: "https://discord.gg/m2aUCxMGC3",
        telegram: "",
        tiktok: "",
        FAQ: [
            [
                "Is Entropy Undetected?",
                "Entropy is fully undetected for Grand Theft Auto Online.",
            ],
            [
                "Partnerships or Reselling",
                "Thank you for your interest in our products. If you want to apply as partner /or and reseller, please contact us on discord. You'll be granted an answer within the fatest delays possible.",
            ],
            [
                "What operating systems does Entropy support?",
                "Our cheats are built and tested for Windows 10 & 11 64 bit system.",
            ],
            [
                "Can I use ShadowPC or another Virtual Machine?",
                "Totally! We know how useful shadow pcs or other providers are for low budget people, or as ban bypass. This is why we decided to support VMS for every Entropy products!",
            ],
        ],
        featuresList: [
            [
                "Online",
                "This category regroup the most useful features in online servers, for PvP, troll and even more!", [
                    [
                        "Local Player",
                        "Ever wanted to be Usain Bolt or being in Godmode? This is the right place for you.",
                    ],
                    [
                        "Player List",
                        "Want to troll players, spectate them, give them weapon and more?",
                    ],
                    [
                        "Vehicle List",
                        "I know how bad you want to repair this broken deluxo, you will now be able to do so!",
                    ],
                    [
                        "Aim Assist",
                        "Having hard time aiming recently? Our aim assist will do the job for you!",
                    ],
                    [
                        "Extinction",
                        "Zombies farming, free antizin and even more, lets destroy extinction together.",
                    ],
                    [
                        "Ped Model Changer",
                        "Ever wanted to become a cow to troll players? This is now possible using Susano!",
                    ],
                ],
            ],
            [
                "Visuals", [
                    "Want to see through walls or want to see enemies weapons? In this section you will find all type of visuals that could fit your requirements.",
                ],
                [
                    ["Entity ESP", "What else to say?"],
                    [
                        "Vehicle ESP",
                        "You want to target that deluxo so bad ehm? We got you..",
                    ],
                ],
            ],
            [
                "Weapon", [
                    "Reloading is annoying right? This recoil on M60 is also really annoying to aim, we know it. Thats why the weapon category exist, it'll fix all your PvP problems.",
                ],
                [
                    [
                        "Weapon Manager",
                        "Manage your recoil, spread, maybe infinite ammos? Who knows.",
                    ],
                    [
                        "Weapon Spawner",
                        "Spending nights on Roleplay servers to get a single glock should be stopped, just spawn em, no?",
                    ],
                ],
            ],
            [
                "Vehicule", [
                    "Ever wanted to become VROOOOOOOOOOOOOM VROOOOOOOOOOOOOM? Well I guess you're at the right place :^)",
                ],
                [
                    [
                        "Vehicle Manager",
                        "Engine boost, vehicle repair, customization and even more?!",
                    ],
                    [
                        "Vehicle Mods",
                        "We know that you want the coolest car and the best upgrades no?",
                    ],
                    [
                        "Vehicle Spawner",
                        "Trying to find good vehicles is boring, no but fr thats why we made this.",
                    ],
                ],
            ],
        ],
        current_feature: [],
    },
    methods: {
        test: function(element) {
            document
                .querySelector(".div9 .div10:nth-child(" + parseInt(element) + ")")
                .classList.toggle("active-faq");
        },
        select_features: function(element) {
            this.current_feature = this.featuresList[element - 1][2];

            for (let index = 1; index <= this.featuresList.length; index++) {
                document
                    .querySelector(".div3 .ul .a2:nth-child(" + index + ")")
                    .classList.remove("active-features");

                document
                    .querySelector(
                        ".div3 .ul .a2:nth-child(" + index + ") .arrow_right-svg"
                    )
                    .classList.remove("active-features-arrow");
            }

            document
                .querySelector(".div3 .ul .a2:nth-child(" + parseInt(element) + ")")
                .classList.toggle("active-features");
            document
                .querySelector(
                    ".div3 .ul .a2:nth-child(" + parseInt(element) + ") .arrow_right-svg"
                )
                .classList.toggle("active-features-arrow");

            document.querySelector(".online2").innerHTML =
                this.featuresList[parseInt(element) - 1][0];

            document.querySelector(".features-desc").innerHTML =
                this.featuresList[parseInt(element) - 1][1];
        },
        openInNewTab(url) {
            window.open("https://discord.gg/m2aUCxMGC3");
        },
        scrollFunction: function() {
            console.log(
                document.querySelector(".frame-368").getBoundingClientRect().y
            );
            window.scrollTo(
                0,
                document.querySelector(".frame-368").getBoundingClientRect().y - 150
            );
        },
    },
    mounted() {
        this.select_features("1");
    },
});

function delay(time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

window.onscroll = function() {
    scrollFunction();
};

async function scrollFunction() {
    let scrollPercent =
        ((document.documentElement.scrollTop + document.body.scrollTop) /
            (document.documentElement.scrollHeight -
                document.documentElement.clientHeight)) *
        100;

    document.querySelector(".rectangle-77").style.height = scrollPercent + "%";
    if (window.scrollY >= 100) {
        document.querySelector(".frame-3").style.display = "block";
        document.querySelector(".frame-3").style.position = "fixed";
        document.querySelector(".frame-365").style.position = "fixed";
        document.querySelector(".frame-366").style.position = "fixed";
        document.querySelector(".logo-1").style.position = "fixed";
        await delay(5);
        document.querySelector(".frame-3").style.padding = "0px 0px";
        document.querySelector(".frame-3").style.height = "100px";
    } else {
        await delay(5);
        document.querySelector(".frame-3").style.padding = "0px 0px";
        document.querySelector(".frame-3").style.height = "0px";
    }
}