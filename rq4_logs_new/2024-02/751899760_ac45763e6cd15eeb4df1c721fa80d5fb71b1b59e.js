// Define acceptable values in a spearate config file or directly within this module 
const genders = ['Male','Female','Non-binary','Genderqueer','Genderfluid'];
const pronouns = ['He/him','She/her','They/them','Ze/hir','Ze/zir','Xe/xem'];
const orientations = ['Homosexual','Bisexual','Asexual','Pansexual','Queer','bi','bi-sexual'];

const validateAge = (line) => {
    const age = parseInt(line.split(':')[1].trim(), 10);
    return age >= 16 && age <= 60;
}

const validateInList = (line, list) => {
    const value = line.split(':')[1].trim();
    return list.includes(value);
}

const validateExists = (line) => line && line.split(':')[1].trim().length > 0;

// Main validation function
const validateIntroMessage = (content) => {
    const lines = content.split('\n').map(line => line.trim());
    let errors = [];

    // Perform validations
    if (!lines.some(line => line.startsWith('Age:') && validateAge(line))) {
        errors.push('Age must be between 16-60.');
    }

    if (!lines.some(line => line.startsWith('Gender:') && validateInList(line, genders))) {
        errors.push('Gender is not recognized.');
    }

    if (!lines.some(line => line.startsWith('Pronouns:') && validateInList(line, pronouns))) {
        errors.push('Pronouns are not recognized.');
    }

    if (!lines.some(line => line.startsWith('Orientation:') && validateInList(line, orientations))) {
        errors.push('Orientation is not recognized.');
    }

    if (!lines.some(line => line.startsWith('Location:') && validateExists(line))) {
        errors.push('Location is required.');
    }

    // Return an object with validation result
    return {
        isValid: errors.length === 0,
        errors
    };
};

module.exports = { validateIntroMessage };