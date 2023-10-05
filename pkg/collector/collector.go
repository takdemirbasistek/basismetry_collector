package collector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/joho/godotenv"
	common "github.com/takdemirbasistek/basismetry_collector/pkg/util"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type Collector struct {
	ServiceName    string
	ServiceVersion string
	Language       string
	DB             *sql.DB
}

var tracer trace.Tracer
var tracerBasismetryProvider *sdktrace.TracerProvider

func New() *Collector {
	return &Collector{}
}

func (c *Collector) CreateTraceProvider() (*sdktrace.TracerProvider, error) {
	var (
		signozToken  = os.Getenv("SIGNOZ_ACCESS_TOKEN")
		collectorURL = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		insecure     = os.Getenv("INSECURE_MODE")
	)

	if strings.TrimSpace(c.ServiceName) == "" {
		c.ServiceName = os.Getenv("SERVICE_NAME")
	}

	if strings.TrimSpace(c.ServiceVersion) == "" {
		c.ServiceVersion = os.Getenv("SERVICE_VERSION")
	}

	if strings.TrimSpace(c.Language) == "" {
		c.Language = os.Getenv("SERVICE_LANGUAGE")
	}

	if strings.TrimSpace(c.ServiceName) == "" {
		pwd, _ := os.Getwd()
		c.ServiceName, _ = filepath.Abs(pwd)
	}

	var headers = make(map[string]string)
	if strings.TrimSpace(signozToken) != "" {
		headers = map[string]string{
			"signoz-access-token": signozToken,
		}
	}

	secureOption := otlptracegrpc.WithTLSCredentials(credentials.NewClientTLSFromCert(nil, "")) // config can be passed to configure TLS
	if len(insecure) > 0 {
		secureOption = otlptracegrpc.WithInsecure()
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracegrpc.NewClient(
			secureOption,
			otlptracegrpc.WithEndpoint(collectorURL),
			otlptracegrpc.WithHeaders(headers),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	resources, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", c.ServiceName),
			attribute.String("library.language", c.Language),
			attribute.String("os", runtime.GOOS),
			attribute.String("service.version", c.ServiceVersion),
		),
	)
	if err != nil {
		log.Fatalf("Could not set resources: %v", err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resources),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tracerProvider, nil
}

func (c *Collector) Init() {
	envFilePath := common.GetEnvFilePath()
	err := godotenv.Load(envFilePath)
	if err != nil {
		return
	}
	tracerBasismetryProvider, err = c.CreateTraceProvider()
	if err != nil {
		return
	}
	tracer = tracerBasismetryProvider.Tracer(c.ServiceName)
}

func (c *Collector) Start(
	ctx context.Context,
	r *http.Request,
	OtherDetails map[string]string,
	Events map[string]string,
) (context.Context, trace.Span) {
	return c.createSpan(ctx, r, OtherDetails, Events)
}

func (c *Collector) End(span trace.Span) {
	if span != nil {
		span.End()
	}
}

func (c *Collector) ErrorStart(
	ctx context.Context,
	r *http.Request,
	err error,
	OtherDetails map[string]string,
	Events map[string]string,
) (context.Context, trace.Span) {
	ctx, span := c.createSpan(ctx, r, OtherDetails, Events)
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return ctx, span
}

type DriverName string

const (
	MySQL      DriverName = "mysql"
	MariaDB    DriverName = "mariadb"
	MSSQL      DriverName = "mssql"
	PostgreSQL DriverName = "postgresql"
	Oracle     DriverName = "oracle"
	MongoDB    DriverName = "mongodb"
	Cassandra  DriverName = "cassandra"
)

func (s DriverName) IsValid() bool {
	switch s {
	case MySQL, PostgreSQL, MariaDB, MSSQL, Oracle, MongoDB, Cassandra:
		return true
	}
	return false
}

func (s DriverName) String() string {
	return string(s)
}

func datasourceName(username, password, host, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, dbName)
}

func (c *Collector) OpenDB(driverName DriverName, username string, password string, host string, dbName string) (*sql.DB, error) {

	if !driverName.IsValid() {
		return nil, errors.New("driver name is invalid")
	}

	var attr attribute.KeyValue

	switch driverName {
	case MySQL:
		attr = semconv.DBSystemMySQL
		break
	case MariaDB:
		attr = semconv.DBSystemMariaDB
		break
	case MSSQL:
		attr = semconv.DBSystemMSSQL
		break
	case PostgreSQL:
		attr = semconv.DBSystemPostgreSQL
		break
	case Oracle:
		attr = semconv.DBSystemOracle
		break
	case MongoDB:
		attr = semconv.DBSystemMongoDB
		break
	case Cassandra:
		attr = semconv.DBSystemCassandra
		break

	}

	// open up our database connection.
	db, err := otelsql.Open(driverName.String(), datasourceName(username, password, host, dbName), otelsql.WithAttributes(
		attr,
	))
	if err != nil {
		return nil, fmt.Errorf("open main db error: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("ping error: %w", err)
	}

	return c.DB, nil
}

func (c *Collector) CloseDB() error {
	err := c.DB.Close()
	if err != nil {
		return err
	}

	return nil
}

func (c *Collector) createSpan(ctx context.Context,
	r *http.Request,
	OtherDetails map[string]string,
	Events map[string]string) (context.Context, trace.Span) {
	var spanContextConfig trace.SpanContextConfig
	/*	spanContextConfig.TraceID, _ = trace.TraceIDFromHex(payload.TraceID)
		if strings.TrimSpace(payload.SpanID) != "" {
			spanContextConfig.SpanID, _ = trace.SpanIDFromHex(payload.SpanID)
		}
		spanContextConfig.TraceFlags = 01
		spanContextConfig.Remote = true*/
	spanContext := trace.NewSpanContext(spanContextConfig)
	ctx = trace.ContextWithSpanContext(ctx, spanContext)

	/*	spanContextConfig.TraceID, _ = trace.TraceIDFromHex(payload.TraceID)
		spanContextConfig.SpanID, _ = trace.SpanIDFromHex(payload.SpanID)*/
	spanContextConfig.TraceFlags = 01
	spanContextConfig.Remote = true
	spanContext = trace.NewSpanContext(spanContextConfig)
	ctx = trace.ContextWithSpanContext(ctx, spanContext)
	//c.SetRequest(c.Request().WithContext(ctx))
	if tracer == nil {
		return nil, nil
	}
	ctx, span := tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.RequestURI))
	span.SetAttributes(attribute.String("http.host", r.Host))
	span.SetAttributes(attribute.String("http.method", r.Method))
	span.SetAttributes(attribute.String("http.uri", r.RequestURI))
	span.SetAttributes(attribute.String("http.remote_addr", r.RemoteAddr))
	span.SetAttributes(attribute.String("http.user_agent", r.UserAgent()))

	if len(OtherDetails) > 0 {
		for k, val := range OtherDetails {
			span.SetAttributes(attribute.String(k, val))
		}
	}
	if len(Events) > 0 {
		for k, val := range Events {
			span.AddEvent(k, trace.WithAttributes(attribute.String(k, val)))
		}
	}

	return ctx, span
}