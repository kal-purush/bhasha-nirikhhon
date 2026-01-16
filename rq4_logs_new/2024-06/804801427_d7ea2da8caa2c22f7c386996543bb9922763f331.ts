import { styled } from 'styled-components';

export const AsideMenuStyled = styled.aside`
    
    .sub-menu__body.spollers__body {
        height: 100%;
        max-height: 0;
        overflow: hidden;
        padding-top: 0;
        padding-bottom: 0;
        transition: all .5s ease;
    }
    .sub-menu__item.spollers__item.active {
        .sub-menu__body.spollers__body {
            max-height: 200px;
            opacity: 1;
            padding-top: 1.25rem;
            padding-bottom: 1.25rem;
        }
    }
    .page-menu__item.item-page-menu {
        position: relative;
    }
    .sub-menu__title {
        border: none;
    }
    .item-page-menu__content.sub-menu .sub-menu__title.spollers__title {
        position: relative;
        padding-right: 0;
        margin-right: 0;
    }
    .item-page-menu__content.sub-menu .sub-menu__title.spollers__title:after,
    .page-menu__item.item-page-menu:after {
        content: "";
        width: 4px;
        height: 100%;
        position: absolute;
        top: 0;
        right: 0;
        background-color: var(--blue);
        opacity: 0;
        transition: all .3s ease;
    }
    .item-page-menu__content.sub-menu.active .sub-menu__title.spollers__title:after,
    .page-menu__item.item-page-menu.active:after {
        opacity: 1;
    }
    .item-page-menu__text.active {
        color: var(--blue);
    }
    /* .page-menu__item.item-page-menu.active {
        border-right: 4px solid var(--blue);
    } */
    .active.item-page-menu .sub-menu__title {
        border: none;
    }
    
`