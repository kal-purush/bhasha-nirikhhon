export class Comment {
    constructor(
        public commentId: number = null,
        public content: string = '',
        public owner = '',
        public _comments = [],
        public _message = '',
        public created_at: Date = new Date(),
        public updated_at: Date = new Date()
    ){}
}