from pydantic import BaseModel, Field

class OrderBase(BaseModel):
    """Contrat de base : les attributs communs à la commande."""
    customer_id: str = Field(..., min_length=3, max_length=64, description="Identifiant unique du client")
    item: str = Field(..., min_length=1, max_length=128, description="Nom ou référence du produit")
    amount: float = Field(..., gt=0, description="Montant total (strictement supérieur à 0)")

class OrderCreate(OrderBase):
    """Schéma d'entrée : ce que le client envoie dans le body du POST /orders."""
    pass

class OrderResponse(OrderBase):
    """Schéma de sortie : ce que notre API renvoie au format JSON."""
    id: int
    status: str

    class Config:
        from_attributes = True