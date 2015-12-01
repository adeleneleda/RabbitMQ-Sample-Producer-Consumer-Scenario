import pika, sys

print "** Message Producer **"

credentials = pika.PlainCredentials("guest", "guest")
conn_params = pika.ConnectionParameters("localhost",
                                        credentials = credentials)

conn_broker = pika.BlockingConnection(conn_params)
channel = conn_broker.channel()


# Declare Exchanges
channel.exchange_declare(exchange="company-fanout-exchange",
                         type="fanout")

channel.exchange_declare(exchange="company-direct-exchange",
                         type="direct")

channel.exchange_declare(exchange="company-topic-exchange",
                         type="topic")



# Declare Corresponding Queues
channel.queue_declare(queue="marketing-dept-queue")

channel.queue_declare(queue="stock-manager-queue")

channel.queue_declare(queue="devops-queue")


# Bind Queues
## FANOUT

# result = channel.queue_declare(auto_delete=True)
# channel.queue_bind(queue=result.method.queue,
#                    exchange='company-fanout-exchange')

## TOPIC
# channel.queue_bind(queue='marketing-dept-queue',
#                    exchange='company-topic-exchange',
#                    routing_key='marketing.*')

# channel.queue_bind(queue='stock-manager-queue',
#                    exchange='company-topic-exchange',
#                    routing_key='stock.*')

# channel.queue_bind(queue='devops-queue',
#                    exchange='company-topic-exchange',
#                    routing_key='*.critical')

# channel.queue_bind(queue='devops-queue',
#                    exchange='company-direct-exchange',
#                    routing_key='URGENT')

msg = sys.argv[1]
msg_props= pika.BasicProperties(delivery_mode = 2)
msg_props.content_type = "text/plain"

if "ALL:" in msg:
  channel.basic_publish(body=msg,
                        exchange="company-fanout-exchange",
                        properties=msg_props,
                        routing_key=msg)
elif msg == "URGENT":
  channel.basic_publish(body=msg,
                        exchange="company-direct-exchange",
                        properties=msg_props,
                        routing_key=msg)
else:
  channel.basic_publish(body=msg,
                        exchange="company-topic-exchange",
                        properties=msg_props,
                        routing_key=msg)


