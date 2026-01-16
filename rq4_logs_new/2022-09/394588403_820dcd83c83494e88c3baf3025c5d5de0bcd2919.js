import(chrome.runtime.getURL("/lib/monkey-script.js")).then(async (Monkey) => {
  Monkey.js(() => {
    const getFirstReportDropdownElement = () =>
      [
        ...document
          .querySelector("one-appnav")
          .shadowRoot.querySelector("one-app-nav-bar")
          .shadowRoot.querySelectorAll("one-app-nav-bar-item-root"),
      ]
        .filter((root) =>
          root.shadowRoot.querySelector("a").href.includes("/Report/")
        )[0]
        ?.shadowRoot?.querySelector("one-app-nav-bar-item-dropdown")
        ?.shadowRoot?.querySelector("one-app-nav-bar-menu-button")
        ?.shadowRoot?.querySelector("slot")
        ?.assignedElements?.()?.[0];

    document.addEventListener("click", (ev) => {
      if (!ev.target.matches("one-appnav")) return;

      const doIt = () => {
        const reportHeader = getFirstReportDropdownElement();
        if (!reportHeader) return;

        if (!reportHeader.matches("p")) return;

        reportHeader.classList.add("slds-has-divider_top-space");
        reportHeader.outerHTML =
          `<one-app-nav-bar-menu-item one-appnavbaritemdropdown_appnavbaritemdropdown="" class="slds-dropdown__item" one-appnavbarmenuitem_appnavbarmenuitem-host="">
            <a one-appnavbarmenuitem_appnavbarmenuitem="" href="/one/one.app#eyJjb21wb25lbnREZWYiOiJyZXBvcnRzOnJlcG9ydEJ1aWxkZXIiLCJhdHRyaWJ1dGVzIjp7InJlY29yZElkIjoiIiwibmV3UmVwb3J0QnVpbGRlciI6dHJ1ZX0sInN0YXRlIjp7fX0%3D" role="menuitem" tabindex="-1" draggable="false">
              <span one-appnavbarmenuitem_appnavbarmenuitem="" class="slds-truncate">
                <lightning-icon one-appnavbarmenuitem_appnavbarmenuitem="" class="slds-icon-text-default slds-m-right--x-small slds-shrink-none slds-icon-utility-add slds-icon_container">
                  <lightning-primitive-icon>
                    <svg class="slds-icon slds-icon-text-default slds-icon_x-small" focusable="false" data-key="add" aria-hidden="true" viewBox="0 0 52 52">
                      <g>
                        <path d="M30 29h16.5c.8 0 1.5-.7 1.5-1.5v-3c0-.8-.7-1.5-1.5-1.5H30c-.6 0-1-.4-1-1V5.5c0-.8-.7-1.5-1.5-1.5h-3c-.8 0-1.5.7-1.5 1.5V22c0 .6-.4 1-1 1H5.5c-.8 0-1.5.7-1.5 1.5v3c0 .8.7 1.5 1.5 1.5H22c.6 0 1 .4 1 1v16.5c0 .8.7 1.5 1.5 1.5h3c.8 0 1.5-.7 1.5-1.5V30c0-.6.4-1 1-1z"/>
                      </g>
                    </svg>
                  </lightning-primitive-icon>
                </lightning-icon>
                <span one-appnavbarmenuitem_appnavbarmenuitem="">Rapportje maken</span>
              </span>
            </a>
          </one-app-nav-bar-menu-item>` + reportHeader.outerHTML;

        const a = getFirstReportDropdownElement().querySelector("a");
        a.addEventListener("click", () => {
          window.location.href = a.href;
        });
      };

      setTimeout(doIt, 150);
      setTimeout(doIt, 300);
      setTimeout(doIt, 600);
      setTimeout(doIt, 1200);
      setTimeout(doIt, 2400);
    });
  });
});