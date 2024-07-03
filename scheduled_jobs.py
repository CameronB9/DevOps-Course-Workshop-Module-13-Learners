from flask import Flask
from data.database import save_order, get_all_orders
from products import create_product_download
from apscheduler.schedulers.background import BackgroundScheduler
import requests

from data.order import COMPLETE, FAILED


def initialise_scheduled_jobs(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=process_orders,
        args=[app],
        trigger="interval",
        seconds=app.config["SCHEDULED_JOB_INTERVAL_SECONDS"],
    )
    scheduler.start()


def process_orders(app: Flask):
    with app.app_context():
        orders = get_queue_of_orders_to_process()
        if len(orders) == 0:
            return

        status = COMPLETE
        order = orders[0]

        if order.status == FAILED:
            return

        app.logger.info(f'Date placed: {order.date_placed.isoformat()}')
        app.logger.info(f'Date placed local: {order.date_placed_local.isoformat()}')

        payload = {
            "product": order.product,
            "customer": order.customer,
            "date": order.date_placed_local.isoformat(),
        }
        response = requests.post(
            app.config["FINANCE_PACKAGE_URL"] + "/ProcessPayment",
            json=payload
        )

        app.logger.info(f'Response from endpoint: {response.text}')

        try:
            response.raise_for_status()
        except:
            app.logger.exception(f'Error processing order {order.id}')
            status = FAILED


        order.set_status(status)
        save_order(order)

def get_queue_of_orders_to_process():
    allOrders = get_all_orders()
    queuedOrders = filter(lambda order: order.date_processed == None, allOrders)
    sortedQueue = sorted(queuedOrders, key= lambda order: order.date_placed)
    return list(sortedQueue)
