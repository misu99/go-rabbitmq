package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/misu99/go-rabbitmq/internal/channelmanager"
)

// 定义一个Queue
func NewQueue(conn *Conn, queue string, optionFuncs ...func(*ConsumerOptions)) error {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return err
	}

	err = chanManager.QosSafe(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return fmt.Errorf("declare qos failed: %w", err)
	}
	err = declareExchange(chanManager, options.ExchangeOptions)
	if err != nil {
		return fmt.Errorf("declare exchange failed: %w", err)
	}
	err = declareQueue(chanManager, options.QueueOptions)
	if err != nil {
		return fmt.Errorf("declare queue failed: %w", err)
	}
	err = declareBindings(chanManager, *options)
	if err != nil {
		return fmt.Errorf("declare bindings failed: %w", err)
	}

	return nil
}

// 清空Queue的消息
func ClearQueue(conn *Conn, queue, msgId string, optionFuncs ...func(*ConsumerOptions)) (int, error) {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return 0, errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return 0, err
	}

	return chanManager.QueuePurgeSafe(queue)
}

// 删除Queue中的消息
func DeleteQueueMsg(conn *Conn, queue, msgId string, optionFuncs ...func(*ConsumerOptions)) error {
	defaultOptions := getDefaultConsumerOptions(queue)
	options := &defaultOptions
	for _, optionFunc := range optionFuncs {
		optionFunc(options)
	}

	if conn.connectionManager == nil {
		return errors.New("connection manager can't be nil")
	}

	chanManager, err := channelmanager.NewChannelManager(conn.connectionManager, options.Logger, conn.connectionManager.ReconnectInterval)
	if err != nil {
		return err
	}

	return chanManager.AckMessageSafe(queue, msgId)
}
