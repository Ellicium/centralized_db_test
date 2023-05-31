from typing import List, Optional
from pydantic import BaseModel

class SupplierCountResponse(BaseModel):
    country: Optional[str] = None
    country_code: Optional[str] = None
    supplier_count: Optional[int] = None

class SupplierCountPost(BaseModel):
    text: Optional[str] = None
    level_1: Optional[str] = None
    level_2: Optional[str] = None
    level_3: Optional[str] = None

class LevelItem(BaseModel):
    label: Optional[str] = None
    value: Optional[str] = None


class FilterResponse(BaseModel):
    message: Optional[str] = None
    level_1: List[LevelItem]
    level_2: List[LevelItem]
    level_3: List[LevelItem]
    

