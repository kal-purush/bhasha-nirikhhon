import {ReactNode} from "react";

export type TTag =
/* Текст */
    | 'p'
    /* SEO */
    | 'span'
    | 'div'
    /* Заголовки */
    | 'h1'
    | 'h2'
    | 'h3'
    | 'h4'
    | 'h5'
    /* Формы */
    | 'label'
    /* SVG */
    | 'text'
    /* Жирность */
    | 'strong'

export interface IText {
    children: ReactNode
    tag?: TTag
    color?: string
    fontWeight?: number
    fontSize?: number
    lineHeight?: number
    margin?: number
    marginTop?: number
    marginBottom?: number
    marginRight?: number
    marginLeft?: number
}