package main

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"

	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

// DataPipelineNotification represents a notification for the data pipeline
type DataPipelineNotification struct {
	PipelineName string `json:"pipeline_name"`
	Status       string `json:"status"`
	Timestamp    int64  `json:"timestamp"`
	Message      string `json:"message"`
}

func main() {
	// Initialize OpenTelemetry
	tp, err := setupTracing()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Println(err)
		}
	}()

	// Create an AWS SNS client
	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")}, nil)
	if err != nil {
		log.Fatal(err)
	}
	svc := sns.New(sess)

	// Create a message printer
	p := message.NewPrinter(language.English)

	// Simulate data pipeline notifications
	notifier := NewNotifier(tp, svc, p)
	notifier.Notify(DataPipelineNotification{
		PipelineName: "my-pipeline",
		Status:       "FAILED",
		Timestamp:    1643723400,
		Message:      "Error processing data",
	})
}

func setupTracing() (*tracesdk.TracerProvider, error) {
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint("http://localhost:14250/api/traces"))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithConfig(tracesdk.Config{DefaultSampler: tracesdk.AlwaysSample()}),
		tracesdk.WithExporter(exp),
	)
	return tp, nil
}

type Notifier struct {
	tp     *tracesdk.TracerProvider
	svc    *sns.SNS
	prt    *message.Printer
}

func NewNotifier(tp *tracesdk.TracerProvider, svc *sns.SNS, prt *message.Printer) *Notifier {
	return &Notifier{tp: tp, svc: svc, prt: prt}
}

func (n *Notifier) Notify(notification DataPipelineNotification) {
	ctx := context.Background()
	span := n.tp.Tracer("my-tracer").StartSpan(ctx, "Notify")
	defer span.End()

	// Create an SNS message
	snsInput := &sns.PublishInput{
		Message:  aws.String(notification.Message),
		Subject:  aws.String("Data Pipeline Notification"),
		TopicArn: aws.String("arn:aws:sns:us-west-2:123456789012:my-topic"),
	}
	_, err := n.svc.PublishWithContext(ctx, snsInput)
	if err != nil {
		log.Fatal(err)
	}

	// Print a message
	n.prt.Println("Notification sent:", notification.PipelineName)

	uuid, err := uuid.NewRandom()
	if err != nil {
		log.Fatal(err)
	}
	span.SetAttributes(semconv.Attribute(string(semconv.AttributeUUID), uuid))
}