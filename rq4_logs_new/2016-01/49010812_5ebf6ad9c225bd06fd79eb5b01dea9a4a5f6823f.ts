module colorMetric {
    //Base
    type porcent = number;


    //Colors Interfaces
    type IHEX = string;

    interface IRGB {
        r: number,
        g: number,
        b: number
    }

    interface IHSL {
        h: porcent,
        s: porcent,
        l: porcent
    }

    //http://stackoverflow.com/questions/5623838/rgb-to-hex-and-hex-to-rgb

    function componentToHex(c: number): string {
        var hex: string = c.toString(16);
        return hex.length == 1 ? "0" + hex : hex;
    }

    export function rgbToHex(rgb: IRGB): IHEX {
        return "#" + componentToHex(rgb.r) + componentToHex(rgb.g) + componentToHex(rgb.b);
    }

    export function hexToRgb(hex: IHEX): IRGB {
        var result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result ? {
            r: parseInt(result[1], 16),
            g: parseInt(result[2], 16),
            b: parseInt(result[3], 16)
        } : null;
    }

    export function rgbToHsl(hue: IRGB): IHSL {

        hue.r /= 255;
        hue.g /= 255;
        hue.b /= 255;

        var max = Math.max(hue.r, hue.g, hue.b),
            min = Math.min(hue.r, hue.g, hue.b);

        var hsl: IHSL = {
            h: (max + min) / 2,
            s: (max + min) / 2,
            l: (max + min) / 2,
        }

        if (max == min) {
            hsl.h = hsl.s = 0; // achromatic
        } else {
            var d: number = max - min;
            hsl.s = hsl.l > 0.5 ? d / (2 - max - min) : d / (max + min);
            switch (max) {
                case hue.r: hsl.h = (hue.g - hue.b) / d + (hue.g < hue.b ? 6 : 0); break;
                case hue.g: hsl.h = (hue.b - hue.r) / d + 2; break;
                case hue.b: hsl.h = (hue.r - hue.g) / d + 4; break;
            }
            hsl.h /= 6;
        }

        return {
            h: hsl.h,
            s: hsl.s,
            l: hsl.l
        };
    }

    export function hslToRgb(hsl: IHSL): IRGB {
        var rgb: IRGB = {
            r: 0,
            b: 0,
            g: 0
        };

        if (hsl.s == 0) {
            rgb.r = rgb.g = rgb.b = hsl.l; // achromatic
        } else {
            var hue2rgb = function hue2rgb(p, q, t) {
                if (t < 0) t += 1;
                if (t > 1) t -= 1;
                if (t < 1 / 6) return p + (q - p) * 6 * t;
                if (t < 1 / 2) return q;
                if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
                return p;
            }

            var q = hsl.l < 0.5 ? hsl.l * (1 + hsl.s) : hsl.l + hsl.s - hsl.l * hsl.s;
            var p = 2 * hsl.l - q;
            rgb.r = hue2rgb(p, q, hsl.h + 1 / 3);
            rgb.g = hue2rgb(p, q, hsl.h);
            rgb.b = hue2rgb(p, q, hsl.h - 1 / 3);
        }

        return {
            r: Math.round(rgb.r * 255),
            g: Math.round(rgb.g * 255),
            b: Math.round(rgb.b * 255)
        };
    }


    export function hexToHsl(hex: IHEX): IHSL {
        return rgbToHsl(hexToRgb(hex));
    }

    export function hslToHex(hsl: IHSL): IHEX {
        return rgbToHex(hslToRgb(hsl));
    }
}