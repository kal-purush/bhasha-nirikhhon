import BasePage from "./BasePage";

export default class Homepage extends BasePage {
    // Header
    public header = this.page.locator("header[class*='header']");
    public headerLogoLink = this.page.locator("a[class='header_logo']");
    public headerLogoImage = this.page.locator("a[class='header_logo'] svg");
    public navigation = this.header.locator("nav[class*='header_nav']");
    public homeLink = this.page.locator("nav a[class*='header-link']");
    public aboutButton = this.navigation.locator("button[appscrollto='aboutSection']");
    public contactsButton = this.navigation.locator("button[appscrollto='contactsSection']");
    public guestLogInButton = this.header.locator("button[class='header-link -guest']");
    public signInButton = this.header.locator("button[class*='header_signin']");

    // Hero section
    public signUpButton = this.page.locator("button[class*='hero-descriptor_btn']");

    protected LOCATORS = {
        // CONTACTS_SECTION_LOCATORS
        contactsSection: "div[id='contactsSection']",
        facebookIcon: "span[class*='icon-facebook']",
        facebookLink: "//span[contains(@class,'icon-facebook')]/parent::a",
        telegramIcon: "span[class*='icon-telegram']",
        telegramLink: "//span[contains(@class,'icon-telegram')]/parent::a",
        youtubeIcon: "span[class*='icon-youtube']",
        youtubeLink: "//span[contains(@class,'icon-youtube')]/parent::a",
        instagramIcon: "span[class*='icon-instagram']",
        instagramLink: "//span[contains(@class,'icon-instagram')]/parent::a",
        linkedinIcon: "span[class*='icon-linkedin']",
        linkedinLink: "//span[contains(@class,'icon-linkedin')]/parent::a",
        contactsLink: "a[class='contacts_link display-4']",
        emailLink: "a[class='contacts_link h4']",

        // Footer
        footer: "footer[class*='footer']",
        footerLogoLink: "a[class='footer_logo']",
        footerLogoImage: "a[class='footer_logo'] svg",
    };
};