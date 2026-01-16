export const required = (value: any) => {
    if (value) return undefined;
    return 'Field is required';
}

export const maxLengthCreator = (maxLength: any) => (value: any) => {
    if (value && value.length > maxLength) return  `Max length is ${maxLength} symbols`;
    return undefined;
}

export const maxLength30 = (value: any) => {
    if (value && value.length > 30) return 'Max length is 30 symbols'
    return undefined;
}