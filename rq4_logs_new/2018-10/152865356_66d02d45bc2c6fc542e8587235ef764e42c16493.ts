import * as puppeteer from "puppeteer";
import * as Listr from "listr";
import { login } from "../step/login";
import { logout } from "../step/logout";
import { checkCourses } from "../step/check-courses";
import { checkAnnouncements } from "../step/check-announcements";
import { showUpdates } from "./show-updates";

export async function checkEdux(): Promise<void> {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();

    const steps = new Listr([
        {
            title: "Login to EDUX",
            task: () => login(page)
        },
        {
            title: "Check courses",
            task: data => checkCourses(page, data)
        },
        {
            title: "Check global announcements",
            task: data => checkAnnouncements(page, data)
        },
        {
            title: "Logout from EDUX",
            task: () => logout(page)
        }
    ]);

    await steps
        .run()
        .then(data => showUpdates(data))
        .catch(err => console.error(err));

    await browser.close().catch(() => console.error("EDUX seems to be down!"));
}