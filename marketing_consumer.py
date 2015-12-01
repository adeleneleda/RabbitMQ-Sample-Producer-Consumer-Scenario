import pika

print "** Marketing Department **"

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials = credentials)

conn_broker = pika.BlockingConnection(conn_params)
channel = conn_broker.channel()


channel.exchange_declare(exchange="company-topic-exchange",
                         type="topic")


channel.queue_declare(queue="marketing-dept-queue")
channel.queue_bind(queue='marketing-dept-queue',
                   exchange='company-topic-exchange',
                   routing_key='marketing.*')

channel.queue_bind(queue="marketing-dept-queue",
                   exchange='company-fanout-exchange')

def msg_consumer(channel, method, header, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if body == "quit":
                channel.basic_cancel(consumer_tag="hello-marketing")
                channel.stop_consuming()
        else:
                print body
        return


channel.basic_consume(msg_consumer,
              queue="marketing-dept-queue",
              consumer_tag="hello-marketing")


channel.start_consuming()
