export function getToday() {
    const today = new Date().toLocaleDateString();
    const dateParts = today.split('/');
    const year = dateParts[2];
    const month = dateParts[0].padStart(2, '0'); // Ensure two-digit month
    const day = dateParts[1].padStart(2, '0');   // Ensure two-digit day

    const formattedDate = `${year}-${month}-${day}`;
    return formattedDate;
}

export function formatDate(date: string) {
    const dateParts = date.split('/');
    const year = dateParts[2];
    const month = dateParts[0].padStart(2, '0'); // Ensure two-digit month
    const day = dateParts[1].padStart(2, '0');   // Ensure two-digit day

    const formattedDate = `${year}-${month}-${day}`;
    return formattedDate;
}