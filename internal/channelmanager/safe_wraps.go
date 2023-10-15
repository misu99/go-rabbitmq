package channelmanager

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumeSafe safely wraps the (*amqp.Channel).Consume method
func (chanManager *ChannelManager) ConsumeSafe(
	queue,
	consumer string,
	autoAck,
	exclusive,
	noLocal,
	noWait bool,
	args amqp.Table,
) (<-chan amqp.Delivery, error) {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.Consume(
		queue,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
}

// QueueDeclarePassiveSafe safely wraps the (*amqp.Channel).QueueDeclarePassive method
func (chanManager *ChannelManager) QueueDeclarePassiveSafe(
	name string,
	durable bool,
	autoDelete bool,
	exclusive bool,
	noWait bool,
	args amqp.Table,
) (amqp.Queue, error) {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.QueueDeclarePassive(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// QueueDeclareSafe safely wraps the (*amqp.Channel).QueueDeclare method
func (chanManager *ChannelManager) QueueDeclareSafe(
	name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table,
) (amqp.Queue, error) {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// ExchangeDeclarePassiveSafe safely wraps the (*amqp.Channel).ExchangeDeclarePassive method
func (chanManager *ChannelManager) ExchangeDeclarePassiveSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.ExchangeDeclarePassive(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// ExchangeDeclareSafe safely wraps the (*amqp.Channel).ExchangeDeclare method
func (chanManager *ChannelManager) ExchangeDeclareSafe(
	name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// QueueBindSafe safely wraps the (*amqp.Channel).QueueBind method
func (chanManager *ChannelManager) QueueBindSafe(
	name string, key string, exchange string, noWait bool, args amqp.Table,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.QueueBind(
		name,
		key,
		exchange,
		noWait,
		args,
	)
}

// QosSafe safely wraps the (*amqp.Channel).Qos method
func (chanManager *ChannelManager) QosSafe(
	prefetchCount int, prefetchSize int, global bool,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.Qos(
		prefetchCount,
		prefetchSize,
		global,
	)
}

// PublishSafe safely wraps the (*amqp.Channel).Publish method
func (chanManager *ChannelManager) PublishSafe(
	exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.Publish(
		exchange,
		key,
		mandatory,
		immediate,
		msg,
	)
}

// NotifyReturnSafe safely wraps the (*amqp.Channel).NotifyReturn method
func (chanManager *ChannelManager) NotifyReturnSafe(
	c chan amqp.Return,
) chan amqp.Return {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.NotifyReturn(
		c,
	)
}

// ConfirmSafe safely wraps the (*amqp.Channel).Confirm method
func (chanManager *ChannelManager) ConfirmSafe(
	noWait bool,
) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.Confirm(
		noWait,
	)
}

// NotifyPublishSafe safely wraps the (*amqp.Channel).NotifyPublish method
func (chanManager *ChannelManager) NotifyPublishSafe(
	confirm chan amqp.Confirmation,
) chan amqp.Confirmation {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.NotifyPublish(
		confirm,
	)
}

// NotifyFlowSafe safely wraps the (*amqp.Channel).NotifyFlow method
func (chanManager *ChannelManager) NotifyFlowSafe(
	c chan bool,
) chan bool {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.NotifyFlow(
		c,
	)
}

// 从队列中确认消息
func (chanManager *ChannelManager) AckMessageSafe(queueName, msgId string) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	msg, ok, err := chanManager.channel.Get(queueName, false)
	if err != nil {
		return err
	}

	for ok {
		if msg.MessageId == msgId { // 消息id匹配
			//chanManager.channel.Ack(msg.DeliveryTag, false)
			chanManager.channel.Nack(msg.DeliveryTag, false, false)
			break
		}

		msg, ok, err = chanManager.channel.Get(queueName, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// 从队列中批量确认消息
func (chanManager *ChannelManager) AckMessagesSafe(queueName string, msgIds []string) error {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	msg, ok, err := chanManager.channel.Get(queueName, false)
	if err != nil {
		return err
	}

	find := func(source []string, value string) bool {
		for _, item := range source {
			if item != "" && value != "" && item == value {
				return true
			}
		}
		return false
	}

	for ok {
		if find(msgIds, msg.MessageId) { // 消息id匹配
			//chanManager.channel.Ack(msg.DeliveryTag, false)
			chanManager.channel.Nack(msg.DeliveryTag, false, false)
			break
		}

		msg, ok, err = chanManager.channel.Get(queueName, false)
		if err != nil {
			return err
		}
	}

	return nil
}

// 清空队列消息
func (chanManager *ChannelManager) QueuePurgeSafe(queueName string) (int, error) {
	chanManager.channelMux.RLock()
	defer chanManager.channelMux.RUnlock()

	return chanManager.channel.QueuePurge(queueName, false)
}
