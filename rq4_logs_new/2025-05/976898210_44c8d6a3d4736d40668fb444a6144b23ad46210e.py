from furiadb import get_connection


def listar_campeonatos():
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT id, colocacao, tier, nome, resultado, premiacao 
    FROM campeonatos
    ORDER BY 
        CASE tier
            WHEN 'S-Tier' THEN 1
            WHEN 'A-Tier' THEN 2
            WHEN 'B-Tier' THEN 3
            ELSE 4
        END,
        CASE 
            WHEN colocacao LIKE '1%' THEN 1
            WHEN colocacao LIKE '2%' THEN 2
            WHEN colocacao LIKE '3%' THEN 3
            ELSE 4
        END
    """)
    
    campeonatos = cursor.fetchall()
    conn.close()
    return campeonatos

def formatar_campeonatos(campeonatos):
    if not campeonatos:
        return "🏆 *Nenhum campeonato registrado ainda!* 🏆"
    
    texto = "✨ *HISTÓRICO DE CONQUISTAS FURIOSAS* ✨\n\n"
    
    tier_emojis = {
        "S-Tier": "💎",
        "A-Tier": "🔥",
        "B-Tier": "⚡",
        "C-Tier": "🎯"
    }
    
    for id_camp, colocacao, tier, nome, resultado, premiacao in campeonatos:
        if "1" in colocacao:
            medalha = "🥇"
            resultado_emoji = "✅ *DOMINÂNCIA TOTAL*"
        elif "2" in colocacao:
            medalha = "🥈"
            resultado_emoji = "⚠️ *FINALISTA*"
        elif "3" in colocacao:
            medalha = "🥉"
            resultado_emoji = "📈 *SEMIFINALISTA*"
        else:
            medalha = "🔹"
            resultado_emoji = "💢 *PARTICIPAÇÃO*"
        
        placar = f"⚔️ *{resultado.replace(':', ' - ')}*" if ":" in resultado else f"🔮 *{resultado}*"
        
        texto += (
            f"{tier_emojis.get(tier, '🏅')} *{tier}*\n"
            f"{medalha} *{colocacao.upper()}* - {nome}\n"
            f"{placar} | {resultado_emoji}\n"
            f"💰 *Premiação:* {premiacao}\n"
            f"────────────────────\n"
        )
    
    return texto