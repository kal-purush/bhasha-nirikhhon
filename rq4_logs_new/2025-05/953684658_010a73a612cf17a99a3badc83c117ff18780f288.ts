function formatDescription(text?: string): string {
    if (!text) return "";

    const sentences = text
        .toLowerCase()
        .split(".")
        .map(sentence => sentence.trim())
        .filter(Boolean)
        .map(sentence => sentence.charAt(0).toUpperCase() + sentence.slice(1));

    return sentences.join(". ") + (text.trim().endsWith(".") ? "." : "");
}

export default formatDescription;