/// <reference types="react" />
import PropTypes from "prop-types";
declare type selectedType = string | number;
interface ITabProps {
    selected: selectedType;
    children?: any;
}
export declare const Tab: {
    ({ selected, children }: ITabProps): JSX.Element;
    propTypes: {
        children: PropTypes.Requireable<string | number | boolean | {} | PropTypes.ReactElementLike | PropTypes.ReactNodeArray>;
        selected: PropTypes.Requireable<number>;
    };
    defaultProps: {
        children: string;
        selected: number;
    };
};
export {};