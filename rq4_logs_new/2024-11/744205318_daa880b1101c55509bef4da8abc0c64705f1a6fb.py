from typing import Dict, List, Optional
from pydantic import BaseModel, Field

class CountryModel(BaseModel):
    """Data model representing country-level research metrics and relationships"""
    
    outputs: Optional[List] = None
    authors: Optional[Dict] = None
    metrics: Optional[Dict] = None
    
    
class CountryNodeModel(BaseModel):
    dbpedia: Optional[str] = None
    id: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    name: Optional[str] = None
    official_name: Optional[str] = None