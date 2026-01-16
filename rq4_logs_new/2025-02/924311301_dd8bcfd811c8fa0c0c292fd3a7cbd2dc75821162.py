import pandas as pd
import re
from globals import capitalizePalabras, removeAccents

"""
Armar metadata y parsear títulos para SQM PT Litio y Nitrato.
"""

# Variable global para recordar la última flota encontrada.
_lastMatch = {"flota": None}

def spotJSON(df: pd.DataFrame, company: str) -> dict:
    """
    Construye un diccionario JSON con la información de los clientes SQM PT Litio o Nitrato.
    Extrae flotas, oficinas y códigos del DataFrame, además de los niveles de riesgo.
    Args:
        df (pd.DataFrame): DataFrame con columnas como 'Flota', 'Patente' y los niveles de riesgo.
        company (str): Nombre del cliente (usado para presentarlo en mayúscula inicial).
    Returns:
        dict: Estructura con la siguiente forma:
            {
                "Cliente": <Nombre capitalizado>,
                "Informacion": {
                    "Niveles de Riesgo RAEV/100": {bajo, medio, alto},
                    "Flotas": [...],
                    "Patentes": [...]
                }
            }
    """
    company = capitalizePalabras(company)
    flotas = sorted(df['Flota'].apply(capitalizePalabras).dropna().unique().tolist())
    patentes = sorted(df['Patente'].dropna().unique().tolist())
    riesgo = {}
    for row in ['bajo', 'medio', 'alto']:
        riesgo[row] = df[row][0]

    configDict = {
        "Cliente": company,
        "Informacion": {
            "Niveles de Riesgo RAEV/100": riesgo,
            "Flotas": flotas,
            "Patentes": patentes,
        },
    }

    return configDict

def parseTitle(title: str, metadata: dict, week: str) -> dict:
    """
    Interpreta el título de una página PDF para determinar el tipo de gráfico y parámetros asociados.
    Usa la metadata y la fecha de la semana para inferir información como:
    - Tipo de gráfico (evolución, ranking, vehículos).
    - Flota, oficina o código detectados en el texto.
    - Rango de fechas (startDate, endDate).
    - Parámetros de visualización (ultimas, top, dev).
    Args:
        title (str): Título extraído del PDF.
        metadata (dict): Estructura de metadatos con flotas, oficinas, códigos y niveles de riesgo.
        week (str): Fecha de inicio de la semana (YYYY-MM-DD).
    Returns:
        dict: Diccionario con claves como 'Tipo', 'Flotas', 'Oficina', 'Patente', 'startDate', 'endDate', etc.
    """
    global _lastMatch
    titleLower = title.lower()
    result = {
        "Tipo": None,
        "Flotas": None,
        "Patente": None,
        "startDate": None,
        "endDate": None,
        "ultimas": None,
        "top": None,
        "dev": None
    }
    
    # Determina el tipo de gráfico
    if "evolución semanal" in titleLower:
        result["Tipo"] = "Evolucion"
    elif any(k in titleLower for k in ["ranking", "variacion", "variación", "evolución"]):
        result["Tipo"] = "Ranking"
    elif "vehículos" in titleLower:
        result["Tipo"] = "Vehiculos"
    else:
        print(f"Tipo de gráfico no reconocido en el título: {title}")
        return result
    
    # Busca coincidencia de 'Flota <nombre>'
    matchFlota = re.search(
        r"flota\s+([A-Za-záéíóúÁÉÍÓÚñÑ]+(?:\s+[A-Za-záéíóúÁÉÍÓÚñÑ]+)*)(?=,|\n)",
        title, 
        re.IGNORECASE
    )
    if matchFlota:
        flotaName = matchFlota.group(1).strip()
        flotaName = removeAccents(flotaName)
        print(f'Flota reconocida sin tilde: {flotaName}')
        for f in metadata["Informacion"]["Flotas"]:
            if f.lower() == flotaName.lower():
                result["Flotas"] = [removeAccents(f).upper()]
                break
        print(f"Match flota: {result['Flotas']}")
        _lastMatch["flota"] = result["Flotas"]
    
    # Si aparece 'Total Flotas'
    if "total flotas" in titleLower and result["Tipo"] == "Evolucion":
        result["Flotas"] = [flota.upper() for flota in metadata["Informacion"]["Flotas"]]
        print(f"Match total flotas: {result['Flotas']}")
    
    # Busca coincidencia de 'Código <valor>'
    matchPatente = re.search(r"patente\s+(\S+)", title, re.IGNORECASE)
    if matchPatente :
        patente = matchPatente.group(1).strip()
        for c in metadata["Informacion"]["Patentes"]:
            if c.lower() == patente.lower():
                result["Patente"] = c
                break
        print(f"Match código: {result['Patente']}")
        if not result["Patente"]:
            print(f"El código {patente} no se encontró en la metadata.")
        if not matchFlota:
            result["Flotas"] = _lastMatch["flota"]
    
    # Define cuantas últimas semanas mostrar
    if matchPatente:
        result["ultimas"] = 4
    if any(k in titleLower for k in ["variacion", "variación"]):
        result["ultimas"] = 2

    # Define parámetros de visualización de más riesgosos
    if any(k in titleLower for k in ["mas riesgosos", "más riesgosos", "más riesgosas" , "mas riesgosas"]):
        result["top"] = 60
        result["dev"] = 0
    
    week = pd.to_datetime(week)
    if result["Tipo"] == "Evolucion":
        result["endDate"] = week
        result["startDate"] = week - pd.DateOffset(years=1)
    if result["Tipo"] in ["Ranking", "Vehiculos"]:
        result["endDate"] = week + pd.DateOffset(days=6)
        result["startDate"] = week
    if result["ultimas"] in [2, 4]:
        result["startDate"] = None
    
    print(f"""
Título: {title}
Parámetros extraídos:
    Tipo: {result["Tipo"]}
    startDate: {result["startDate"]}
    endDate: {result["endDate"]}
    Flotas: {result["Flotas"]}
    Patente: {result["Patente"]}
    ultimas: {result["ultimas"]}
    top: {result["top"]}
    dev: {result["dev"]}
    """)
    
    return result