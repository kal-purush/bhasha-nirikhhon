import {$,nest,rem,stylesheet} from "@barlus/styles"
import { taglVariant, taglBase } from '../mixins/tag';

export default Theme;
export const enum Theme {
    tag='tag',
    tagError='tag-error',
    tagPrimary='tag-primary',
    tagRounded='tag-rounded',
    tagSecondary='tag-secondary',
    tagSuccess='tag-success',
    tagWarning='tag-warning',
}

stylesheet('tags.ts')('',{
    ...nest([`.${Theme.tag}`],{
        ...taglBase(),
        ...taglVariant($.bodyFontColor.lighten(0.05),$.bgColorDark),
        display:'inline-block',
        ...nest([`&.${Theme.tagRounded}`],{
            borderRadius:rem(5),
            paddingLeft:rem(.4),
            paddingRight:rem(.4),
        }),
        ...nest([`&.${Theme.tagPrimary}`],{
            ...taglVariant($.lightColor,$.primaryColor),
        }),
        ...nest([`&.${Theme.tagSecondary}`],{
            ...taglVariant($.primaryColor,$.secondaryColor),
        }),
        ...nest([`&.${Theme.tagSuccess}`],{
            ...taglVariant($.lightColor,$.successColor),
        }),
        ...nest([`&.${Theme.tagWarning}`],{
            ...taglVariant($.lightColor,$.warningColor),
        }),
        ...nest([`&.${Theme.tagError}`],{
            ...taglVariant($.lightColor,$.errorColor),
        }),
    }),
});