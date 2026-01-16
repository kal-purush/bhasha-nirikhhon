const statusMapping: { [key: string]: string } = {
    PLANNED: "Geplant",
    CREATED: "Erstellt",
    SENT: "Gesendet",
    CONFIRMED: "Bestätigt",
    UNDER_REVIEW: "In Prüfung",
    INVITATION: "Einladung",
    ACCEPTED: "Zusage",
    REJECTED: "Abgesagt",
    WITHDRAWN: "Zurückgezogen",
    ARCHIVED: "Archiviert"
};

export default function translateStatusToGerman(status: string): string {
    // Übersetze den Status, falls möglich, ansonsten gib den Status direkt zurück
    return statusMapping[status] || status;
};