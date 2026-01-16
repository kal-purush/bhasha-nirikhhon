import { test } from "@playwright/test";

test.describe("Go Easy @location @advertisement @access @advertiser @admin", () => {
  test("create location with an advertisement and then delete it", async ({
    browser,
  }) => {
    const adminContext = await browser.newContext({
      storageState: "./playwright/.auth/admin.json",
    });
    const adminPage = await adminContext.newPage();
    const advertiserContext = await browser.newContext({
      storageState: "./playwright/.auth/user.json",
    });
    const advertiserPage = await advertiserContext.newPage();

    //Admin changing access level on user to granted
    await adminPage.goto("http://localhost:3000");
    await adminPage.getByRole("button", { name: "Anmodninger" }).click();
    await adminPage
      .locator(
        'tr:has-text("Go Easy Test") td[test-column="access"] input:not([type="hidden"])'
      )
      .click();
    await adminPage.getByRole("option", { name: "Granted" }).click();

    //User creating location
    await advertiserPage.goto("http://localhost:3000");
    await advertiserPage.getByRole("button", { name: "Lokationer" }).click();
    await advertiserPage.getByRole("link", { name: "Ny lokation" }).click();
    await advertiserPage
      .getByPlaceholder("Peppas pizza")
      .fill("Test Lokations navn");
    await advertiserPage
      .getByPlaceholder("Vejgade 21.")
      .fill("tornerosevej 51");
    await advertiserPage.getByPlaceholder("Vejgade 21.").click({ delay: 1500 });
    await advertiserPage
      .getByRole("button", { name: "51, Tornerosevej, Herlevhuse, Herlev" })
      .click({ timeout: 15000 });
    await advertiserPage.getByPlaceholder("12345678").fill("12345678");
    await advertiserPage
      .getByPlaceholder("https://google.com/")
      .fill("hjemmeside.com");
    await advertiserPage.getByPlaceholder("Resume").fill("kort resume");
    await advertiserPage.getByPlaceholder("Beskrivelse").fill("en beskrivelse");
    await advertiserPage
      .getByPlaceholder("picture.com/cat.jpg")
      .fill("https://i.imgur.com/wbVVXHob.jpg");
    await advertiserPage.getByRole("button", { name: "Opret" }).click();

    //User creating an advertisement linked to the lokation
    await advertiserPage.waitForURL("/locations");
    await advertiserPage.goto("/advertisements");
    await advertiserPage.waitForURL("/advertisements");
    await advertiserPage.getByRole("link", { name: "Ny annonce" }).click();
    await advertiserPage.getByLabel("Vælg lokation *").click();
    await advertiserPage
      .getByRole("option", { name: "Test Lokations navn" })
      .click();
    await advertiserPage
      .getByPlaceholder("Peppes pizza")
      .fill("test annonce navn");
    await advertiserPage
      .getByPlaceholder("Byens bedste pizza")
      .fill("test annonce beskrivelse");
    await advertiserPage
      .getByPlaceholder("https://i.imgur.com/uvFEcJN.jpeg")
      .fill("https://i.imgur.com/uvFEcJN.jpeg");
    await advertiserPage.getByRole("button", { name: "Opret" }).click();

    //User updating location
    await advertiserPage.goto("/locations");
    await advertiserPage.waitForURL("/locations");
    await advertiserPage.locator("tr td a").nth(2).click();
    await advertiserPage.getByPlaceholder("Peppas pizza").clear();
    await advertiserPage
      .getByPlaceholder("Peppas pizza")
      .fill("Updated test lokation navn");
    await advertiserPage.getByPlaceholder("https://google.com/").clear();
    await advertiserPage
      .getByPlaceholder("https://google.com/")
      .fill("https://testupdatehjemmeside.com");
    await advertiserPage.getByRole("button", { name: "Opdatér" }).click();

    //Admin going to user location table
    await adminPage.goto("/advertisers");
    await adminPage.waitForURL("/advertisers");
    await adminPage.locator('tr:has-text("Go Easy Test") td a').click();

    //Admin going to an user's location's advertisements
    await adminPage.locator("tr td a").nth(2).click();

    //User deleting location
    await advertiserPage.goto("/locations");
    await advertiserPage.waitForURL("/locations");
    await advertiserPage.locator("tr td a").nth(2).click();
    await advertiserPage.getByRole("button", { name: "Slet" }).click();

    //Admin changing users access back to pending
    await adminPage.goto("/advertisers");
    await adminPage.waitForURL("/advertisers");
    await adminPage
      .locator(
        'tr:has-text("Go Easy Test") td[test-column="access"] input:not([type="hidden"])'
      )
      .click();
    await adminPage.getByRole("option", { name: "Pending" }).click();
  });
});