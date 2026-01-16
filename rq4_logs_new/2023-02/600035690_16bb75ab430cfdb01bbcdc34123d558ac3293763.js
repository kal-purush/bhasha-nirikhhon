import Link from "next/link"
export default function Page404() {
    return ( 
        <div style={{"text-align": "center", "color": "white"}}>
            <h1>404</h1>
            <h2>Página não encontrada</h2>
            <h2>Retorne para a página principal</h2>
            <Link href="/" style={{color:"inherit"}}>Clique aqui</Link>
        </div>
)
}