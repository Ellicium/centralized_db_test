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


class SupplierInfo(BaseModel):
    supplier : Optional[str] = None
    category : Optional[str] = None
    region : Optional[str] = None
    level_1: Optional[str] = None
    level_2: Optional[str] = None
    level_3: Optional[str] = None
    text: Optional[str] = None


class SupplierInfoResponse(BaseModel):
    ap_supplier_id: Optional[str] = None
    Supplier_Name:  Optional[str] = None
    country:  Optional[str] = None
    Supplier_Capability:  Optional[str] = None
    level1:  Optional[str] = None
    level2:  Optional[str] = None
    level3:  Optional[str] = None

class SupplierCategoryWise(BaseModel):
    level_1 :Optional[str] = None
    level_2 :Optional[str] = None
    level_3 :Optional[str] = None
    category_text :Optional[str] = None

class SupplierDetails(BaseModel):
    supplier_id :Optional[int] = None


class LevelItem(BaseModel):
    label: Optional[str] = None
    value: Optional[str] = None

class FilterResponse(BaseModel):
    message: Optional[str] = None
    level_1: List[LevelItem]
    level_2: List[LevelItem]
    level_3: List[LevelItem]
    
