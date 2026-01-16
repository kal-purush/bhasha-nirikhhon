export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      COMMENT: {
        Row: {
          author: string
          COMMENT_TO: string
          CONTENT: string
          id: string
          PUBLISHED_AT: string
        }
        Insert: {
          author: string
          COMMENT_TO: string
          CONTENT: string
          id?: string
          PUBLISHED_AT?: string
        }
        Update: {
          author?: string
          COMMENT_TO?: string
          CONTENT?: string
          id?: string
          PUBLISHED_AT?: string
        }
        Relationships: [
          {
            foreignKeyName: "COMMENT_author_fkey"
            columns: ["author"]
            isOneToOne: false
            referencedRelation: "USER"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "COMMENT_COMMENT_TO_fkey"
            columns: ["COMMENT_TO"]
            isOneToOne: false
            referencedRelation: "POST"
            referencedColumns: ["id"]
          }
        ]
      }
      FOLLOWER: {
        Row: {
          follower: string
          FOLLOWING: string
        }
        Insert: {
          follower: string
          FOLLOWING: string
        }
        Update: {
          follower?: string
          FOLLOWING?: string
        }
        Relationships: [
          {
            foreignKeyName: "FOLLOWER_follower_fkey"
            columns: ["follower"]
            isOneToOne: false
            referencedRelation: "USER"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "FOLLOWER_FOLLOWING_fkey"
            columns: ["FOLLOWING"]
            isOneToOne: false
            referencedRelation: "USER"
            referencedColumns: ["id"]
          }
        ]
      }
      POST: {
        Row: {
          author: string
          CONTENT: string
          id: string
          LIKES: number
          PUBLISHED_AT: string
        }
        Insert: {
          author: string
          CONTENT: string
          id?: string
          LIKES: number
          PUBLISHED_AT?: string
        }
        Update: {
          author?: string
          CONTENT?: string
          id?: string
          LIKES?: number
          PUBLISHED_AT?: string
        }
        Relationships: [
          {
            foreignKeyName: "POST_author_fkey"
            columns: ["author"]
            isOneToOne: false
            referencedRelation: "USER"
            referencedColumns: ["id"]
          }
        ]
      }
      USER: {
        Row: {
          BACKGROUND_COLOR: string
          DISPLAY_NAME: string
          id: string
          USERNAME: string
        }
        Insert: {
          BACKGROUND_COLOR: string
          DISPLAY_NAME: string
          id: string
          USERNAME: string
        }
        Update: {
          BACKGROUND_COLOR?: string
          DISPLAY_NAME?: string
          id?: string
          USERNAME?: string
        }
        Relationships: [
          {
            foreignKeyName: "USER_id_fkey"
            columns: ["id"]
            isOneToOne: true
            referencedRelation: "users"
            referencedColumns: ["id"]
          }
        ]
      }
    }
    Views: {
      [_ in never]: never
    }
    Functions: {
      [_ in never]: never
    }
    Enums: {
      [_ in never]: never
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

export type Tables<
  PublicTableNameOrOptions extends
    | keyof (Database["public"]["Tables"] & Database["public"]["Views"])
    | { schema: keyof Database },
  TableName extends PublicTableNameOrOptions extends { schema: keyof Database }
    ? keyof (Database[PublicTableNameOrOptions["schema"]]["Tables"] &
        Database[PublicTableNameOrOptions["schema"]]["Views"])
    : never = never
> = PublicTableNameOrOptions extends { schema: keyof Database }
  ? (Database[PublicTableNameOrOptions["schema"]]["Tables"] &
      Database[PublicTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : PublicTableNameOrOptions extends keyof (Database["public"]["Tables"] &
      Database["public"]["Views"])
  ? (Database["public"]["Tables"] &
      Database["public"]["Views"])[PublicTableNameOrOptions] extends {
      Row: infer R
    }
    ? R
    : never
  : never

export type TablesInsert<
  PublicTableNameOrOptions extends
    | keyof Database["public"]["Tables"]
    | { schema: keyof Database },
  TableName extends PublicTableNameOrOptions extends { schema: keyof Database }
    ? keyof Database[PublicTableNameOrOptions["schema"]]["Tables"]
    : never = never
> = PublicTableNameOrOptions extends { schema: keyof Database }
  ? Database[PublicTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : PublicTableNameOrOptions extends keyof Database["public"]["Tables"]
  ? Database["public"]["Tables"][PublicTableNameOrOptions] extends {
      Insert: infer I
    }
    ? I
    : never
  : never

export type TablesUpdate<
  PublicTableNameOrOptions extends
    | keyof Database["public"]["Tables"]
    | { schema: keyof Database },
  TableName extends PublicTableNameOrOptions extends { schema: keyof Database }
    ? keyof Database[PublicTableNameOrOptions["schema"]]["Tables"]
    : never = never
> = PublicTableNameOrOptions extends { schema: keyof Database }
  ? Database[PublicTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : PublicTableNameOrOptions extends keyof Database["public"]["Tables"]
  ? Database["public"]["Tables"][PublicTableNameOrOptions] extends {
      Update: infer U
    }
    ? U
    : never
  : never

export type Enums<
  PublicEnumNameOrOptions extends
    | keyof Database["public"]["Enums"]
    | { schema: keyof Database },
  EnumName extends PublicEnumNameOrOptions extends { schema: keyof Database }
    ? keyof Database[PublicEnumNameOrOptions["schema"]]["Enums"]
    : never = never
> = PublicEnumNameOrOptions extends { schema: keyof Database }
  ? Database[PublicEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : PublicEnumNameOrOptions extends keyof Database["public"]["Enums"]
  ? Database["public"]["Enums"][PublicEnumNameOrOptions]
  : never