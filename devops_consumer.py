import pika

print "** Devops Department **"

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials = credentials)

conn_broker = pika.BlockingConnection(conn_params)
channel = conn_broker.channel()

channel.queue_declare(queue="devops-queue")

channel.exchange_declare(exchange="company-direct-exchange",
                         type="direct")

channel.exchange_declare(exchange="company-topic-exchange",
                         type="topic")

channel.queue_bind(queue='devops-queue',
                   exchange='company-topic-exchange',
                   routing_key='*.critical')

channel.queue_bind(queue='devops-queue',
                   exchange='company-direct-exchange',
                   routing_key='URGENT')

channel.queue_bind(queue="devops-queue",
                   exchange='company-fanout-exchange')

def msg_consumer(channel, method, header, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if body == "quit":
                channel.basic_cancel(consumer_tag="hello-devops")
                channel.stop_consuming()
        else:
                print body
        return


channel.basic_consume(msg_consumer,
                      queue="devops-queue",
                      consumer_tag="hello-devops")

channel.start_consuming()
