from os import name
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Depends
from typing import List
from gateapi.api import schemas
from gateapi.api.dependencies import get_rpc, config
from .exceptions import OrderNotFound, OrderInvalidQueryParam

router = APIRouter(
    prefix="/orders",
    tags=['Orders']
)


@router.get("", status_code=status.HTTP_200_OK, response_model=schemas.ListOrdersSuccess)
def list_orders(page: int = 1, limit: int = 5, rpc=Depends(get_rpc)):
    try:
        return _list_order(page, limit, rpc)
    except OrderNotFound as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(error)
        )
    except OrderInvalidQueryParam as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(error)
        )


def _list_order(page, limit, nameko_rpc):
    # Retrieve order data from the orders service.
    # Note - this may raise a remote exception that has been mapped to
    # raise``OrderNotFound`` or raise``OrderInvalidQueryParam``
    with nameko_rpc.next() as nameko:
        orders = nameko.orders.list(page, limit)

    if len(orders['data']) == 0:
        return orders

    # Retrieve all order products from the products service
    with nameko_rpc.next() as nameko:
        order_product_ids = list(
            set([item['product_id'] for order in orders['data'] for item in order['order_details']]))
        product_map = {
            prod['id']: prod for prod in nameko.products.list(order_product_ids)}

    # get the configured image root
    image_root = config['PRODUCT_IMAGE_ROOT']

    # Enhance order details with product and image details.
    for order in orders['data']:
        for item in order['order_details']:
            product_id = item['product_id']

            item['product'] = product_map[product_id]
            # Construct an image url.
            item['image'] = '{}/{}.jpg'.format(image_root, product_id)

    return orders


@router.get("/{order_id}", status_code=status.HTTP_200_OK)
def get_order(order_id: int, rpc=Depends(get_rpc)):
    try:
        return _get_order(order_id, rpc)
    except OrderNotFound as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(error)
        )


def _get_order(order_id, nameko_rpc):
    # Retrieve order data from the orders service.
    # Note - this may raise a remote exception that has been mapped to
    # raise``OrderNotFound``
    with nameko_rpc.next() as nameko:
        order = nameko.orders.get(order_id)

    # Retrieve all order products from the products service
    with nameko_rpc.next() as nameko:
        order_product_ids = list(
            set(item['product_id'] for item in order['order_details']))
        product_map = {
            prod['id']: prod for prod in nameko.products.list(order_product_ids)}

    # get the configured image root
    image_root = config['PRODUCT_IMAGE_ROOT']

    # Enhance order details with product and image details.
    for item in order['order_details']:
        product_id = item['product_id']

        item['product'] = product_map[product_id]
        # Construct an image url.
        item['image'] = '{}/{}.jpg'.format(image_root, product_id)

    return order


@router.post("", status_code=status.HTTP_200_OK, response_model=schemas.CreateOrderSuccess)
def create_order(request: schemas.CreateOrder, rpc=Depends(get_rpc)):
    id_ = _create_order(request.dict(), rpc)
    return {
        'id': id_
    }


def _create_order(order_data, nameko_rpc):
    # check order product ids are valid
    with nameko_rpc.next() as nameko:
        order_product_ids = list(
            set(item['product_id'] for item in order_data['order_details']))
        valid_product_ids = {prod['id']
                             for prod in nameko.products.list(order_product_ids)}

        for item in order_data['order_details']:
            if item['product_id'] not in valid_product_ids:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                    detail=f"Product with id {item['product_id']} not found"
                                    )
        # Call orders-service to create the order.
        result = nameko.orders.create(
            order_data['order_details']
        )
        return result['id']
