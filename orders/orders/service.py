from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession

from orders.exceptions import NotFound, InvalidQueryParam
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema
import math


class OrdersService:
    name = 'orders'

    db = DatabaseSession(DeclarativeBase)
    event_dispatcher = EventDispatcher()

    @rpc
    def get(self, order_id):
        order = self.db.query(Order).get(order_id)

        if not order:
            raise NotFound('Order with id {} not found'.format(order_id))

        return OrderSchema().dump(order).data

    @rpc
    def list(self,  page: int = 1, limit: int = 5):
        if page < 1:
            raise InvalidQueryParam(
                'Invalid request "page" should be greater or equal 1')

        if limit < 1:
            raise InvalidQueryParam(
                'Invalid request "limit" should be greater or equal 1')

        offset = (page - 1) * limit

        orders = self.db.query(Order).limit(limit).offset(offset)
        total = self.db.query(Order).count()

        total_pages = math.ceil(total / limit)
        has_next = page < total_pages

        return {
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": total_pages,
            "has_next": has_next,
            "data": OrderSchema(many=True).dump(orders).data
        }

    @rpc
    def create(self, order_details):
        order = Order(
            order_details=[
                OrderDetail(
                    product_id=order_detail['product_id'],
                    price=order_detail['price'],
                    quantity=order_detail['quantity']
                )
                for order_detail in order_details
            ]
        )
        self.db.add(order)
        self.db.commit()

        order = OrderSchema().dump(order).data

        self.event_dispatcher('order_created', {
            'order': order,
        })

        return order

    @rpc
    def update(self, order):
        order_details = {
            order_details['id']: order_details
            for order_details in order['order_details']
        }

        order = self.db.query(Order).get(order['id'])

        for order_detail in order.order_details:
            order_detail.price = order_details[order_detail.id]['price']
            order_detail.quantity = order_details[order_detail.id]['quantity']

        self.db.commit()
        return OrderSchema().dump(order).data

    @rpc
    def delete(self, order_id):
        order = self.db.query(Order).get(order_id)
        self.db.delete(order)
        self.db.commit()
