import * as opn from "opn";

// opens the image in the default image viewer
opn("unicorn.png").then(() => {
    // image viewer closed
});

// opens the image in the default image viewer
opn("unicorn.png", { wait: false }).then(cp => {
    setTimeout(() => cp.kill(), 500);
});

// opens the url in the default browser
opn("http://sindresorhus.com");

// specify the app to open in
opn("http://sindresorhus.com", { app: "firefox" });

// specify app arguments
opn("http://sindresorhus.com", { app: ["google chrome", "--incognito"] });