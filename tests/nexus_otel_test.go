package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type headerGetter interface {
	Get(string) string
}

type NexusOTELSuite struct {
	parallelsuite.Suite[*NexusOTELSuite]
}

func TestNexusOTELSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusOTELSuite{})
}

func (s *NexusOTELSuite) newTestEnv(exporter sdktrace.SpanExporter) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true,
		testcore.WithSpanExporter(exporter),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
	)
}

// Verifies production callback wiring propagates trace context and stored headers end to end.
func (s *NexusOTELSuite) TestCallback() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	requestHeaders := make(chan headerGetter, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	s.T().Cleanup(server.Close)

	callbackWorker := worker.New(env.SdkClient(), env.Tv().TaskQueue().GetName(), worker.Options{})
	callbackWorker.RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: env.Tv().WorkflowType().GetName()},
	)
	s.NoError(callbackWorker.Start())
	s.T().Cleanup(callbackWorker.Stop)

	callbackHeaderValue := env.Tv().Any().String()
	startResponse, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          env.Tv().RequestID(),
		Namespace:          env.Namespace().String(),
		WorkflowId:         env.Tv().WorkflowID(),
		WorkflowType:       env.Tv().WorkflowType(),
		TaskQueue:          env.Tv().TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute),
		Identity:           env.Tv().Any().String(),
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: server.URL,
					Header: map[string]string{
						"X-Callback-Header": callbackHeaderValue,
					},
				},
			},
		}},
	})
	s.NoError(err)
	s.NoError(env.SdkClient().GetWorkflow(s.Context(), env.Tv().WorkflowID(), startResponse.RunId).Get(s.Context(), nil))

	// Wait for the Nexus callback.
	headers := s.requireExportedClientSpan(exporter, requestHeaders)
	s.Equal(callbackHeaderValue, headers.Get("X-Callback-Header"))
}

func (s *NexusOTELSuite) TestExternalOperation() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	// Verifies external Nexus operations use the instrumented production HTTP client.
	s.Run("Start", func(s *NexusOTELSuite) {
		tv := env.Tv().Sub("Start")
		requestHeaders := make(chan headerGetter, 1)
		endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
			OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				requestHeaders <- options.Header
				return &nexus.HandlerStartOperationResultSync[any]{Value: tv.Any().String()}, nil
			},
		})

		_, err := env.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:              env.Namespace().String(),
			OperationId:            tv.Any().String(),
			Endpoint:               endpointName,
			Service:                tv.Service(),
			Operation:              tv.Operation(),
			RequestId:              tv.RequestID(),
			ScheduleToCloseTimeout: durationpb.New(time.Minute),
		})
		s.NoError(err)
		s.requireExportedClientSpan(exporter, requestHeaders)
	})

	// Verifies asynchronous operation cancellation uses the instrumented production HTTP client.
	s.Run("Cancel", func(s *NexusOTELSuite) {
		tv := env.Tv().Sub("Cancel")
		cancelRequestHeaders := make(chan headerGetter, 1)
		operationToken := tv.Any().String()
		endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
			OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultAsync{OperationToken: operationToken}, nil
			},
			OnCancelOperation: func(_ context.Context, _, _, _ string, options nexus.CancelOperationOptions) error {
				cancelRequestHeaders <- options.Header
				return nil
			},
		})

		operationID := tv.Any().String()
		startResponse, err := env.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:              env.Namespace().String(),
			OperationId:            operationID,
			Endpoint:               endpointName,
			Service:                tv.Service(),
			Operation:              tv.Operation(),
			RequestId:              tv.RequestID(),
			ScheduleToCloseTimeout: durationpb.New(time.Minute),
		})
		s.NoError(err)

		_, err = env.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       startResponse.RunId,
			WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
		})
		s.NoError(err)
		_, err = env.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
			Namespace:   env.Namespace().String(),
			OperationId: operationID,
			RunId:       startResponse.RunId,
			Reason:      tv.Any().String(),
		})
		s.NoError(err)
		s.requireExportedClientSpan(exporter, cancelRequestHeaders)
	})
}

func (s *NexusOTELSuite) TestWorkerOperation() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)

	// Verifies worker-target Nexus operations connect local frontend client and server spans.
	s.Run("ByEndpoint", func(s *NexusOTELSuite) {
		tv := env.Tv().Sub("ByEndpoint").WithTaskQueue(env.WorkerTaskQueue())
		requestHeaders := make(chan headerGetter, 1)
		service := nexus.NewService("test-service")
		operation := nexus.NewSyncOperation("test-operation", func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (string, error) {
			requestHeaders <- options.Header
			return tv.Any().String(), nil
		})
		service.MustRegister(operation)

		nexusWorker := worker.New(env.SdkClient(), tv.TaskQueue().GetName(), worker.Options{})
		nexusWorker.RegisterNexusService(service)
		s.NoError(nexusWorker.Start())
		s.T().Cleanup(nexusWorker.Stop)

		endpoint := env.createNexusEndpoint(s.Context(), s.T(), testcore.RandomizedNexusEndpoint(s.T().Name()), tv.TaskQueue().GetName())
		_, err := env.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
			Namespace:              env.Namespace().String(),
			OperationId:            tv.Any().String(),
			Endpoint:               endpoint.GetSpec().GetName(),
			Service:                service.Name,
			Operation:              operation.Name(),
			RequestId:              tv.RequestID(),
			ScheduleToCloseTimeout: durationpb.New(time.Minute),
		})
		s.NoError(err)
		headers := s.requireExportedClientSpan(exporter, requestHeaders)
		s.requireExportedServerSpan(exporter, headers, "DispatchNexusTaskByEndpoint", "io.temporal.frontend")
	})

	// Verifies the namespace and task queue dispatch route is instrumented independently of forwarding.
	s.Run("ByNamespaceAndTaskQueue", func(s *NexusOTELSuite) {
		tv := env.Tv().Sub("ByNamespaceAndTaskQueue")
		taskQueue := tv.RequestID()
		pollerErrCh := env.nexusTaskPoller(s.Context(), s.T(), taskQueue, nexusEchoHandler)
		client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL: getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue),
			Service: "test-service",
		})
		s.NoError(err)

		requestHeaders := nexus.Header{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		}
		_, err = nexusrpc.StartOperation(s.Context(), client, op, tv.Any().String(), nexus.StartOperationOptions{
			Header: requestHeaders,
		})
		s.NoError(err)
		s.NoError(<-pollerErrCh)
		s.requireExportedServerSpan(exporter, requestHeaders, "DispatchNexusTaskByNamespaceAndTaskQueue", "io.temporal.frontend")
	})
}

func (s *NexusOTELSuite) requireExportedClientSpan(
	exporter *tracetest.InMemoryExporter,
	requestHeaders <-chan headerGetter,
) headerGetter {
	var headers headerGetter
	select {
	case headers = <-requestHeaders:
	case <-s.Context().Done():
		s.FailNow("timed out waiting for Nexus request", s.Context().Err().Error())
		return nil
	}
	traceID, spanID := s.requireTraceContext(headers)
	var exportedSpan tracetest.SpanStub
	s.AwaitTrue(func() bool {
		for _, span := range exporter.GetSpans() {
			if span.SpanKind == oteltrace.SpanKindClient &&
				span.SpanContext.TraceID() == traceID &&
				span.SpanContext.SpanID() == spanID {
				exportedSpan = span
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	s.requireSpanServiceName(exportedSpan, "io.temporal.history")
	return headers
}

func (s *NexusOTELSuite) requireExportedServerSpan(
	exporter *tracetest.InMemoryExporter,
	headers headerGetter,
	operation string,
	serviceName string,
) {
	traceID, clientSpanID := s.requireTraceContext(headers)
	var exportedSpan tracetest.SpanStub
	s.AwaitTrue(func() bool {
		for _, span := range exporter.GetSpans() {
			if span.Name == operation &&
				span.SpanKind == oteltrace.SpanKindServer &&
				span.SpanContext.TraceID() == traceID &&
				span.Parent.SpanID() == clientSpanID {
				exportedSpan = span
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	s.requireSpanServiceName(exportedSpan, serviceName)
}

func (s *NexusOTELSuite) requireSpanServiceName(span tracetest.SpanStub, expected string) {
	s.Require().NotNil(span.Resource)
	serviceName, ok := span.Resource.Set().Value(semconv.ServiceNameKey)
	s.Require().True(ok)
	s.Require().Equal(expected, serviceName.AsString())
}

func (s *NexusOTELSuite) requireTraceContext(headers headerGetter) (oteltrace.TraceID, oteltrace.SpanID) {
	// Extract trace context from headers.
	traceparent := strings.Split(headers.Get("traceparent"), "-")
	s.Len(traceparent, 4)
	traceID, err := oteltrace.TraceIDFromHex(traceparent[1])
	s.NoError(err)
	spanID, err := oteltrace.SpanIDFromHex(traceparent[2])
	s.NoError(err)
	return traceID, spanID
}
