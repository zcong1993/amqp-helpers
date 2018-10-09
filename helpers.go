package helpers

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// MustDeclareConn create a connection or die
func MustDeclareConn(url string) *amqp.Connection {
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}

// MustDeclareExchange declare an exchange channel or die
func MustDeclareExchange(conn *amqp.Connection, exchange string, args amqp.Table) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	failOnError(err, "Failed to declare an exchange")

	return ch
}

// MustDeclareConsumer declare a consumer delivery chan or die
func MustDeclareConsumer(ch *amqp.Channel, exchange, qName string, routerKeys []string, args amqp.Table) (*amqp.Channel, <-chan amqp.Delivery) {
	q, err := ch.QueueDeclare(
		qName, // name
		false, // durable
		false, // delete when usused
		false,  // exclusive
		false, // no-wait
		args,  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for _, s := range routerKeys {
		log.Printf("Binding queue %s to exchange %s with routing key %s",
			q.Name, exchange, s)

		err := ch.QueueBind(
			q.Name,   // queue name
			s,        // routing key
			exchange, // exchange
			false,
			nil,
		)
		failOnError(err, "Failed to bind a queue")
	}

	err = ch.Qos(
		5,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	failOnError(err, "Failed to register a consumer")

	return ch, msgs
}

// MustBindQueue bind queue to exchange or die
func MustBindQueue(ch *amqp.Channel, exchange, qName string, routerKeys []string, args amqp.Table) {
	q, err := ch.QueueDeclare(
		qName, // name
		true,  // durable
		false, // delete when usused
		false,  // exclusive
		false, // no-wait
		args,  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	for _, s := range routerKeys {
		log.Printf("Binding queue %s to exchange %s with routing key %s",
			q.Name, exchange, s)

		err := ch.QueueBind(
			q.Name,   // queue name
			s,        // routing key
			exchange, // exchange
			false,
			nil,
		)

		failOnError(err, "Failed to bind a queue")
	}

}

// CopyMsgToPublishing copy an Delivery msg into Publishing
func CopyMsgToPublishing(msg amqp.Delivery) *amqp.Publishing {
	return &amqp.Publishing{
		Headers:         msg.Headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserId,
		AppId:           msg.AppId,
		Body:            msg.Body,
	}
}

// AmqpDeath is struct of "x-death" header
type AmqpDeath struct {
	Count              int       `json:"count"`
	Exchange           string    `json:"exchange"`
	OriginalExpiration string    `json:"original-expiration"`
	Queue              string    `json:"queue"`
	Reason             string    `json:"reason"`
	RoutingKeys        []string  `json:"routing-keys"`
	Time               time.Time `json:"time"`
}

// Headers is simple header struct we need
type Headers struct {
	XDeath []AmqpDeath `json:"x-death"`
}

// ParseDeathHeader parse "x-death" header from raw
func ParseDeathHeader(hs amqp.Table) *AmqpDeath {
	js, err := json.Marshal(hs)
	if err != nil {
		return nil
	}

	var h Headers
	err = json.Unmarshal(js, &h)
	if err != nil {
		return nil
	}

	if len(h.XDeath) == 0 {
		return nil
	}
	return &h.XDeath[0]
}
