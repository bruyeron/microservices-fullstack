from sqlalchemy import String, Float, Integer
from sqlalchemy.orm import Mapped, mapped_column
from database import Base

class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    item: Mapped[str] = mapped_column(String(128), nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="PENDING", nullable=False)