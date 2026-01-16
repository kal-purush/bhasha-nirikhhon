import { ButtonProps } from "@aarock/ui-core";
export type DateFieldProps = ButtonProps & {
    icon?: string;
    format?: string;
    value?: string;
    onValueChange?: (newValue: string) => void;
};
export default function DateField({ icon, value, format, onValueChange, ...props }: DateFieldProps): any;
//# sourceMappingURL=address-field.d.ts.map