package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"kafka-notification-system/pkg/models"
	"log"
	"net/http"
	"sync"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8088"
	KafkaServerAddress = "localhost:9092"
)

type Consumer struct {
	store *NotificationStore
}

func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var notification models.Notification

		userID := string(msg.Key)

		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification: %v", err)
			continue
		}

		consumer.store.Add(userID, notification)
		session.MarkMessage(msg, "")
	}

	return nil
}

type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userID]
}

func (ns *NotificationStore) Add(userID string, notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.data[userID] = append(ns.data[userID], notification)
}

func initConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	group, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to init consumer group: %w", err)
	}

	return group, nil
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	// Step 1 : Init consumer group
	consumerGroup, err := initConsumerGroup()
	if err != nil {
		log.Printf("initialize error: %w", err)
	}

	consumer := &Consumer{
		store: store,
	}

	// Step 2 : Consume Consumer Group
	for {
		err := consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("error from consumer: %w", err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func getUserIDFromRequest(ctx *gin.Context) (string, error) {
	userID := ctx.Param("userID")
	if userID == "" {
		return "", errors.New("no messages found")
	}

	return userID, nil
}

func handleNotificationsHandler(ctx *gin.Context, store *NotificationStore) {
	// Step 1 : Get user id from request
	userID, err := getUserIDFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": err.Error(),
		})
		return
	}

	// Step 2 : Get user's notifications from store
	notifications := store.Get(userID)
	if len(notifications) == 0 {
		ctx.JSON(http.StatusOK, gin.H{
			"message":       "No notifications found for this user",
			"notifications": []models.Notification{},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"notifications": notifications,
	})
}

func main() {
	// Step 1: Init notifications store
	store := &NotificationStore{
		data: make(UserNotifications),
	}

	// Step 2: Setup consumer group
	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)
	defer cancel()

	// Step 3: Setup router with handler
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/notifications/:userID", func(ctx *gin.Context) {
		// Handle notifications handler
		handleNotificationsHandler(ctx, store)
	})

	fmt.Printf("Kafka consumer (Group: %s), started at http://localhost%s\n", ConsumerGroup, ConsumerPort)

	// Step 4: Run router
	if err := router.Run(ConsumerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
