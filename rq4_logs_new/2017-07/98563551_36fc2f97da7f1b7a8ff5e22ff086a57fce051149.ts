export interface User {
    id: string;
    last_name: string;
    first_name: string;
    email: string;
}

export interface Feedback {
    id: string;
    type: string;
    url: string;
    do_not_contact: string;
    status: string;
    user: User;
    comment: string;
    date: string;
    assigned_to: string;
    product: string;
}