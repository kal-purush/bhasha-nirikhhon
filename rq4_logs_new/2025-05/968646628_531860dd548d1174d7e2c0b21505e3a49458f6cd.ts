export function capitalizeWords(text: string): string {
    return text.replace(/\b\w/g, (char) => char.toUpperCase());
}