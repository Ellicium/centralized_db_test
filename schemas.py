from typing import List
from pydantic import BaseModel

class SupplierCountResponse(BaseModel):
    country: str = None
    country_code: str = None
    supplier_count: int = None

class SupplierCountPost(BaseModel):
    text: str = None
    level_1: str = None
    level_2: str = None
    level_3: str = None

class LevelItem(BaseModel):
    label: str = None
    value: str = None


class FilterResponse(BaseModel):
    message: str = None
    level_1: List[LevelItem]
    level_2: List[LevelItem]
    level_3: List[LevelItem]
    

