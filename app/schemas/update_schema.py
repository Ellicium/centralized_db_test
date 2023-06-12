from typing import List, Optional
from pydantic import BaseModel

class UpdateSupplier(BaseModel):
    Shipper_Id:Optional[int] = None
    Description:Optional[str] = None
    Capabilities:Optional[str] = None
    Additional_Notes:Optional[str] = None
    Supplier_Type:Optional[str] = None
    Preferred_Supplier:Optional[int] = None
    User:Optional[str] = None