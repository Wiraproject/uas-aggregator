from sqlalchemy import Column, Integer, String, DateTime, JSON, UniqueConstraint
from sqlalchemy.sql import func
from database import Base

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, index=True, nullable=False)
    event_id = Column(String, index=True, nullable=False)
    timestamp = Column(String, nullable=False)
    source = Column(String, nullable=True)
    payload = Column(JSON, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        UniqueConstraint('topic', 'event_id', name='uq_topic_event_id'),
    )

    def __repr__(self):
        return f"<Event(topic={self.topic}, id={self.event_id})>"