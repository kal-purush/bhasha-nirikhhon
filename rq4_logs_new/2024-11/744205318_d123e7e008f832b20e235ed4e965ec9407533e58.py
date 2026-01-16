from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class AuthorModel(BaseModel):
    """
    Data model representing an academic author or contributor
    with their associated metadata and relationships.
    """

    uuid: str = Field(..., description="Unique identifier for the author")
    orcid: Optional[str] = Field(None, description="Author's ORCID identifier")
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    affiliations: Optional[Dict[str, str]] = Field(
        default=None, description="Author's institutional affiliations"
    )
    workstreams: Optional[Dict[str, str]] = Field(
        default=None, description="Research workstreams the author is involved in"
    )
    collaborators: Optional[List[str]] = Field(
        default=None, description="List of collaborator UUIDs"
    )
    outputs: Optional[List[str]] = Field(
        default=None, description="List of research output UUIDs"
    )