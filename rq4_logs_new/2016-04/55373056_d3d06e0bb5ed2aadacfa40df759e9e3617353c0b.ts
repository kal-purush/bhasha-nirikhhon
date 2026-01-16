// requires react and moment

import * as React from 'react';
import * as moment from 'moment';

declare module "react-bootstrap-datetimepicker" {
    // Import React
    import DateTimeFieldProps = ReactBootstrapDateTimePicker.DateTimeFieldProps;

    // <DateTimeField />
    // ----------------------------------------
    interface DateTimeFieldClass extends React.ComponentClass<DateTimeFieldProps> { }
    var DateTimeField: DateTimeFieldClass;

    export = DateTimeField;
}

declare module ReactBootstrapDateTimePicker {
    interface DateTimeFieldProps extends React.Props<DateTimeField>{
        // Optional
        dateTime?: string;
        format?: string;
        inputFormat?: string;
        onChange?: (value: string) => void;
        showToday?: boolean;
        size?: string;
        daysOfWeekDisabled?: number[];
        viewMode?: string | number;
        inputProps?: React.HTMLProps<HTMLInputElement>;
        minDate?: moment.Moment;
        maxDate?: moment.Moment;
        mode?: "date" | "time";
        defaultText?: string;
    }
    interface DateTimeField extends React.ReactElement<DateTimeFieldProps> { }
}