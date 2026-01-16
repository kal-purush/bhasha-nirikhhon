export interface user {
    error: number;
    data: Array<{
        id: number;
        name: string;
        email: string;
        role: string;
    }>
}