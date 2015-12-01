import pika

print "** Stock Department **"

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials = credentials)

conn_broker = pika.BlockingConnection(conn_params)
channel = conn_broker.channel()

channel.exchange_declare(exchange="company-topic-exchange",
                         type="topic")


channel.queue_declare(queue="stock-manager-queue")

channel.queue_bind(queue='stock-manager-queue',
                   exchange='company-topic-exchange',
                   routing_key='stock.*')

channel.queue_bind(queue="stock-manager-queue",
                   exchange='company-fanout-exchange')


def msg_consumer(channel, method, header, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if body == "quit":
                channel.basic_cancel(consumer_tag="hello-stock")
                channel.stop_consuming()
        else:
                print body
        return

channel.basic_consume(msg_consumer,
                      queue="stock-manager-queue",
                      consumer_tag="hello-stock")


channel.start_consuming()
