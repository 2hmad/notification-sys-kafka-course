package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"kafka-notification-system/pkg/models"
	"log"
	"net/http"
	"strconv"
)

const (
	ProducerPort = ":8089"
	KafkaTopic   = "notifications"
	KafkaAddress = "localhost:9092"
)

func getIDFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))
	if err != nil {
		return 0, err
	}

	return id, nil
}

func findUserByID(userID int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == userID {
			return user, nil
		}
	}

	return models.User{}, errors.New("user not found")
}

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, ctx *gin.Context, fromID, toID int) error {
	message := ctx.PostForm("message")

	fromUser, err := findUserByID(fromID, users)
	if err != nil {
		return fmt.Errorf("failed to get fromUser: %w", err)
	}

	toUser, err := findUserByID(toID, users)
	if err != nil {
		return fmt.Errorf("failed to get toUser: %w", err)
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}

	notificationJson, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to convert notification: %w", err)
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJson),
	}

	partition, offset, err := producer.SendMessage(producerMsg)

	log.Print(partition)
	log.Print(offset)

	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromID, err := getIDFromRequest("fromID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err.Error(),
			})
		}

		toID, err := getIDFromRequest("toID", ctx)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": err.Error(),
			})
		}

		err = sendKafkaMessage(producer, users, ctx, fromID, toID)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{
			"message": "Notification sent successfully!",
		})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	// Step 1 : Init configuration
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{KafkaAddress}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}

	return producer, nil
}

func main() {
	// Step 1 : Init users
	users := []models.User{
		{
			ID:    1,
			Name:  "John",
			Email: "john@example.com",
		},
		{
			ID:    2,
			Name:  "Jane",
			Email: "jane@example.com",
		},
		{
			ID:    3,
			Name:  "Jack",
			Email: "jack@example.com",
		},
	}

	// Step 2 : Setup Producer
	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to init producer: %v", err)
	}
	defer producer.Close()

	// Step 3 : Build send message endpoint
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka producer started at http://localhost%s\n", ProducerPort)

	// Step 4 : Run the router
	err = router.Run(ProducerPort)
	if err != nil {
		log.Fatalf("failed to run the server: %v", err)
	}
}
