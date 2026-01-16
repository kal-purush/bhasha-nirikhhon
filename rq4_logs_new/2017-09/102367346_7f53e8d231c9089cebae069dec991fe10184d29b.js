import styled, { injectGlobal, css } from 'styled-components';
import Font from 'fonts/Font.ttf';

export const media = {
    phone: (...args) => css`
        @media (max-width: 640px) {
            ${ css(...args) }
        }
    `,
    desktop: (...args) => css`
        @media (min-width: 641px) {
            ${ css(...args) }
        }
    `
};

injectGlobal`
    html, body {
    }
    @font-face {
        font-family: Font;
        src: url('${Font}') format('opentype');
    }
    @keyframes flash {
        0%, 100% {
            opacity: 0;
        }
        50% {
            opacity: 1;
        }
    }
`;

/*
 * Mixins
 */

// Box Model

export const getDisplay = p => {
    let display;
    if (p.flex) display = 'flex';
    else if (p.inlineblock) display = 'inline-block';
    else display = 'block';
    return `display: ${display}`;
};

export const getFlexControls = p => {
    let flexDirection = p.rowreverse ? 'row-reverse' : null;
    let alignItems = p.centerboth || p.aligncenter ? 'center' : 'none';
    let justifyContent;
    if (p.centerboth || p.justifycenter) justifyContent = 'center';
    else if (p.justifyspacebetween) justifyContent = 'space-between';
    return `flex-direction: ${flexDirection}; align-items: ${alignItems}; justify-content: ${justifyContent};`;
};

export const getElementSizing = p => {
    const width = `width: ${p.width};`;
    const height = `height: ${p.height};`;
    const minwidth = `min-width: ${p.minwidth};`;
    const minheight = `min-height: ${p.minheight};`;
    const maxwidth = `max-width: ${p.maxwidth};`;
    const maxheight = `max-height: ${p.maxheight}`;
    return `${width} ${height} ${minwidth} ${minheight} ${maxwidth} ${maxheight}`;
}

// Font

export const getFont = p => {
    let font;
    if (p.title) font = 'TimesNewRoman';
    else font = 'TimesNewRoman';
    return `font-family: ${font}`;
};

export const getFontSize = p => {
    let size;
    switch(p.fontsize) {
        case 'title':
            size = '40px';
        case 'subtitle':
            size = '32px';
        case 'lead':
            size = '24px';
        case 'small':
            size = '12px';
        case 'xsmall':
            size = '8px';
        default:
            size = '16px';
    }
    return `font-size: ${size};`;
};

export const getFontWeight = p => {
    let weight;
    switch(p.fontweight) {
        case 'bold':
            size = 'bold';
        default:
            size = 'unset';
    }
    return `font-weight: ${weight};`;
};

export const getTextAlign = p => {
    let textAlign = p.center ? 'center' : 'none';
    return `text-align: ${textalign};`;
};

// Colours / Background

export const getColour = p => {
    let colour;
    switch (p.colour) {
        case 'blue':
            colour = 'blue';
        case 'red':
            colour = 'red';
        case 'white':
            colour = 'white';
        default:
            colour = 'rgba(0,0,0,0.7)';
    }
    return `color: ${colour};`;
};

export const getBGColour = p => {
    let colour;
    switch (p.bgc) {
        case 'slate':
            colour = 'rgba(61, 73, 95, 0.75)';
        default:
            return '';
    }
    return `background-color: ${colour}`;
};

export const getBackgroundImage = p => {
    return `background-image: ${p => p.bgi ? `url(${p.bgi})` : null}`;
}

/*
 * Layout Elements
 */
export const Container = styled.div`
    ${getDisplay}
    ${getBGColour};
    ${getBackgroundImage}
    ${getFlexControls}
    max-width: 1124px;
    margin: 0 auto;
    padding: 0 24px;
`;

export const Wrapper = styled.div`
    ${getDisplay}
    ${getBGColour};
    ${getBackgroundImage}
    ${getFlexControls}
    ${getElementSizing}
    * {
        margin-left: ${p => p.spacechildren ? '24px' : 'none'};
    }
    *:first-child {
        margin-left: ${p => p.spacechildren ? '0px' : 'none'};
    }
    margin: ${p => {
        if (p.margin) return '24px 0';
        else if (p.thinmargin) return '16px 0';
        else return 'none';
    }};
    padding: ${p => {
        if (p.padding) return '24px 0';
        else if (p.thinpadding) return '16px 0';
        else return 'none';
    }};
    overflow: ${p => p.overflowauto ? 'auto' : 'none'};
`;

/*
 * Text Elements
 */
export const Title = styled.h1`
    ${getFont}
    ${getFontSize}
    ${getColour};
    margin: 24px 0 8px;
`;

export const SubTitle = styled.h3`
    ${getFont}
    ${getFontSize}
    ${getColour};
    font-size: 32px;
    margin: 16px 0 8px;
`;

export const P = styled.p`
    ${getFont}
    ${getFontSize}
    ${getFontWeight}
    ${getColour};
    margin: 8px 0;
    ${media.phone`
        text-align: ${p => p.center ? 'center' : 'justify'};
    `}
    // must come after media query
    ${getTextAlign}
`;

export const Text = styled.span`
    ${getFont}
    ${getFontSize}
    ${getFontWeight}
    ${getColour};
    ${getTextAlign}
`;