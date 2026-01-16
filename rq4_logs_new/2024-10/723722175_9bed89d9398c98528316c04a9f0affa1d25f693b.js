export const PI = 3.14159;

function getCircleArea(radius) {
    return PI * radius * radius;
}

function getCircleCircumference(radius) {
    return 2 * PI * radius;
}

export { getCircleArea, getCircleCircumference };