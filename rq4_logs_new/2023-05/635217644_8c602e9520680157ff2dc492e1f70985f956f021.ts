export default interface User {
     id: number
     created_at: string | null
     email: string
     email_verified_at: string | null
     name: string
     related_id: number
     role_id: number
     status: boolean
     updated_at: string
}