import {AppConstants} from '@app/core/AppConstants';
import {AppState} from '@app/core/interfaces/AppState';
import {PageContent} from '@app/core/models/PageContent';
import {NavItem} from '@app/core/interfaces/NavItem';

export function navItemGetter(state: AppState): NavItem[] | null {
    const navItems: NavItem[] = [];
    let pages: {[page: string]: PageContent};

    if (state.pages) {
        pages = state.pages;
    } else {
        return null;
    }

    AppConstants.pinnedPages.forEach((pageId: string) => {
        navItems.push({
            url: pageId,
            label: pageId,
            icon: pages[pageId].icon
        });
    });

    Object.keys(pages).sort().forEach((pageId: string) => {
        if (AppConstants.pinnedPages.indexOf(pageId) === -1 && AppConstants.anchoredPages.indexOf(pageId)) {
            navItems.push({
                url: `page/${pageId}`,
                label: pageId,
                icon: pages[pageId].icon
            });
        }
    });

    AppConstants.anchoredPages.forEach((pageId: string) => {
        navItems.push({
            url: pageId,
            label: pageId,
            icon: pages[pageId].icon
        });
    });

    return navItems;
}