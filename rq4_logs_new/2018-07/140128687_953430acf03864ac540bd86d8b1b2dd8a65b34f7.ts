interface Application {
    readonly id: number,
    readonly name: string,
    readonly url: string,
    readonly client_key: string,
    readonly created_at: string,
    readonly updated_at: string,
    readonly last_probe_received_at: string,
    readonly repo_name: string,  "example: genintho/thomas"
}

interface CSSFileDefinition {
    readonly id: number,
    readonly name: string,
    readonly pattern: string
}

interface CSSFileVariation {
    readonly id: number,
    readonly application_id: number,
    readonly css_file_id: number,
    readonly url: string,
}

interface CSSSelector {
    readonly id: number,
    readonly selector: string,
    readonly updated_at: Date,
    readonly application_id // Can be null to mark not in the join Table
}