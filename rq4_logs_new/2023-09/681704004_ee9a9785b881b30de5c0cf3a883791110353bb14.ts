type FormTextField = 'EMAIL' | 'PASSWORD';

const getRegExp = (type: FormTextField) => {
	switch (type) {
		case 'EMAIL':
			return /^[\w-.]+@([\w-]+\.)+[\w-]{2,4}$/g;
		case 'PASSWORD':
			return /^((?=.*[\d])(?=.*[a-z])(?=.*[A-Z])|(?=.*[a-z])(?=.*[A-Z])(?=.*[^\w\d\s])|(?=.*[\d])(?=.*[A-Z])(?=.*[^\w\d\s])|(?=.*[\d])(?=.*[a-z])(?=.*[^\w\d\s])).{8,30}$/gm;
	}
};

export const validateTextField = (type: FormTextField, field: string) => {
	const regexp = getRegExp(type);
	const isFieldEmpty = field.length <= 0;
	const isFieldValid = regexp.test(field);
	return !isFieldEmpty && isFieldValid;
};