export interface VectorIndexResponseSchema {
    id: string
    name: string
    description?: string
    collection_id: string
    status: string
    created_at: string
    updated_at: string
    document_count: number
    chunk_count: number
    error_message?: string
    embedding_model: string
    vector_db: string
}

export interface DocumentCollectionResponseSchema {
    id: string
    name: string
    description?: string
    status: string
    created_at: string
    updated_at: string
    document_count: number
    chunk_count: number
    error_message?: string
}

export interface ProcessingProgressSchema {
    id: string
    status: string
    progress: number
    current_step: string
    total_files?: number
    processed_files?: number
    total_chunks?: number
    processed_chunks?: number
    error_message?: string
    created_at: string
    updated_at: string
}

export interface RetrievalResultSchema {
    text: string
    score: number
    metadata: {
        document_id: string
        chunk_id: string
        document_title?: string
        page_number?: number
        chunk_number?: number
    }
}

export interface RetrievalResponseSchema {
    results: RetrievalResultSchema[]
    total_results: number
}