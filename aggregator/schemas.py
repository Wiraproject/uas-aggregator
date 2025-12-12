from pydantic import BaseModel
from typing import Optional, List, Any

class EventBase(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str | None = None
    payload: dict | Any | None = None

class EventCreate(EventBase):
    pass

class EventResponse(EventBase):
    id: int
    
    class Config:
        from_attributes = True