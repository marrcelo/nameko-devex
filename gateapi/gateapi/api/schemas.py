from pydantic import BaseModel
from typing import List


class Product(BaseModel):
    id: str
    title: str
    passenger_capacity: int
    maximum_speed: int
    in_stock: int


class CreateOrderDetail(BaseModel):
    product_id: str
    price: float
    quantity: int


class CreateOrder(BaseModel):
    order_details: List[CreateOrderDetail]


class OrderDetail(BaseModel):
    product_id: str
    price: float
    quantity: int
    id: int
    product: Product
    image: str


class Order(BaseModel):
    id: str
    order_details: List[OrderDetail]


class ListOrdersSuccess(BaseModel):
    data: List[Order]
    page: int
    limit: int
    total: int
    total_pages: int
    has_next: bool


class CreateOrderSuccess(BaseModel):
    id: int


class CreateProductSuccess(BaseModel):
    id: str
