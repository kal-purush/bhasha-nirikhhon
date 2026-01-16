import "./navigation.less";
import LiteEvent from "./event";
import {Scale} from "./layout";

export var PageVisible: LiteEvent<HTMLElement>;
export var PageBeforeVisible: LiteEvent<HTMLElement>;
export var PageHidden: LiteEvent<HTMLElement>;
export var GoNextPage: LiteEvent<void>;

export class Carousel {

    private pageBeingHidden: HTMLElement;

    public constructor(parent: HTMLElement) {
        PageVisible = new LiteEvent<HTMLElement>();
        PageBeforeVisible = new LiteEvent<HTMLElement>();
        PageHidden = new LiteEvent<HTMLElement>();

        parent.insertAdjacentHTML("afterbegin", "<div id='pages-carousel'></div>");
        const carousel = document.getElementById("pages-carousel");

        const pages = document.getElementsByClassName("bloom-page");

        //REVIEW: doesn't work. we want to do this so that we can
        // get it visible and determine the size of a page for setting the scale
        pages[0].classList.add("currentPage");

        for (let index = 0; index < pages.length; index++) {
            carousel.appendChild(pages[index]);
        }
    }

    public showFirstPage() {
        [].forEach.call(document.body.querySelectorAll(".bloom-page"), function(page){
                page.classList.remove("currentPage");
            });

        this.firstPage().classList.add("currentPage");
        this.carousel().style.left = "0px";
        // No animation of showing first page, but seems safest to
        // raise this event anyway.
        PageBeforeVisible.raise(this.firstPage());
        PageVisible.raise(this.firstPage());
    }

    public  gotoNextPage(): void {
         this.transitionPage(this.currentPage().nextElementSibling  as HTMLElement, true);
    }

    public  gotoPreviousPage(): void {
        const target = this.currentPage().previousElementSibling  as HTMLElement;
        if (target.classList.contains("bloom-page")) {
            this.transitionPage(target, false);
        }
    }

    private carousel(): HTMLElement {
        return  document.getElementById("pages-carousel");
    }
    private  currentPage(): HTMLElement {
        return this.carousel().getElementsByClassName("currentPage")[0] as HTMLElement;
    }
    private  firstPage(): HTMLElement {
        return document.body.getElementsByClassName("bloom-page")[0] as HTMLElement;
    }

    private  pageWidth(): number {
        return this.currentPage().scrollWidth;
    }

    private  transitionPage(targetPage: HTMLElement, goForward: boolean): void {
        const current = this.currentPage();
        // Here we set a new 'left' or 'right' and will animate transition from 0 to that new value.
        // As soon as it is done, an 'transtionend' event will just hide that page completely
        // and reset the carousel's positioning to 'left:0' (regardless of whether we were manipulating
        // the 'left' (to go forward) or 'right' (to go backward)).
        this.carousel().style.transition = ""; // "left 1.5s ease 0s, right 1.5s ease 0s";

        const amount = Math.round(this.pageWidth());
        assertIsNumber(amount);

        if (goForward) {
            //For some reason, browsers won't do transition effects from "auto" to some pixel value,
            //so we first have to change from "auto" to "0"
            this.carousel().style.left = "0";

            //That needs to get registered before we then change it, so we defer the change
            window.setTimeout(() => {
                  this.carousel().style.transition = "left .5s";
                  this.carousel().style.left = (-1 * amount) + "px";
            }, 100);
        } else {
            //need to change our right so that
            this.carousel().style.left = (-1 * amount) + "px";
            window.setTimeout(() => {
                this.carousel().style.transition = "left .5s";
                this.carousel().style.left = "0px";
            }, 100);
        }

        this.pageBeingHidden = current;

        PageHidden.raise(current);

        //find the next one
        if (targetPage) {
            targetPage.classList.add("currentPage");
            PageBeforeVisible.raise(targetPage); // before the animation
        } else {
            // wrap around //TODO remove this when we can disable the "next button" on the last page
            this.showFirstPage();
        }

        window.setTimeout(() => {
                        this.pageBeingHidden.classList.remove("currentPage");
                        this.carousel().style.transition = "";
                        this.carousel().style.left = "0px";

                        if (current) {
                            PageVisible.raise(current);
                        }
        }, 1000);
    }
}

function assertIsNumber(n: number) {
    if ( !(typeof n === "number" && !isNaN(n))) {
        debugger;
    }
}