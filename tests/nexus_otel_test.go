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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/frontend/configs"
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
func (s *NexusOTELSuite) TestWorkflowCompletionCallback() {
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

// Verifies asynchronous start and cancellation connect real History client and Frontend server spans.
func (s *NexusOTELSuite) TestOperation() {
	callerExporter := tracetest.NewInMemoryExporter()
	callerEnv := s.newTestEnv(callerExporter)
	handlerExporter := tracetest.NewInMemoryExporter()
	handlerEnv := s.newTestEnv(handlerExporter)
	tv := callerEnv.Tv()
	handlerTaskQueue := handlerEnv.Tv().TaskQueue().GetName()

	handlerWorkflow := func(ctx workflow.Context, _ nexus.NoValue) (nexus.NoValue, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return nil, nil
	}
	operation := temporalnexus.NewWorkflowRunOperation(
		"test-operation",
		handlerWorkflow,
		func(_ context.Context, _ nexus.NoValue, options nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{
				ID:        options.RequestID,
				TaskQueue: handlerTaskQueue,
			}, nil
		},
	)
	service := nexus.NewService("test-service")
	service.MustRegister(operation)
	handlerWorker := worker.New(handlerEnv.SdkClient(), handlerTaskQueue, worker.Options{})
	handlerWorker.RegisterWorkflow(handlerWorkflow)
	handlerWorker.RegisterNexusService(service)
	s.NoError(handlerWorker.Start())
	s.T().Cleanup(handlerWorker.Stop)

	handlerWorkerEndpoint := handlerEnv.createNexusEndpoint(
		s.Context(),
		s.T(),
		testcore.RandomizedNexusEndpoint(s.T().Name()),
		handlerTaskQueue,
	)
	callerExternalEndpoint := callerEnv.createExternalNexusEndpoint(
		s.Context(),
		s.T(),
		handlerEnv.getDispatchByEndpointURL(handlerWorkerEndpoint.Id),
	)
	operationID := tv.Any().String()
	startResponse, err := callerEnv.FrontendClient().StartNexusOperationExecution(s.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              callerEnv.Namespace().String(),
		OperationId:            operationID,
		Endpoint:               callerExternalEndpoint,
		Service:                service.Name,
		Operation:              operation.Name(),
		RequestId:              tv.RequestID(),
		ScheduleToCloseTimeout: durationpb.New(time.Minute),
	})
	s.NoError(err)
	s.requireExportedNexusHTTPSpanPairs(callerExporter, handlerExporter, 1)

	pollResponse, err := callerEnv.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   callerEnv.Namespace().String(),
		OperationId: operationID,
		RunId:       startResponse.RunId,
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)
	s.Require().Equal(enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED, pollResponse.GetWaitStage())
	_, err = callerEnv.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
		Namespace:   callerEnv.Namespace().String(),
		OperationId: operationID,
		RunId:       startResponse.RunId,
		Reason:      tv.Any().String(),
	})
	s.NoError(err)
	s.requireExportedNexusHTTPSpanPairs(callerExporter, handlerExporter, 2)
}

// Verifies the namespace and task queue dispatch route is instrumented independently of forwarding.
func (s *NexusOTELSuite) TestNamespaceAndTaskQueueDispatch() {
	exporter := tracetest.NewInMemoryExporter()
	env := s.newTestEnv(exporter)
	tv := env.Tv()
	taskQueue := tv.TaskQueue().GetName()
	pollerErrCh := env.nexusTaskPoller(s.Context(), s.T(), taskQueue, nexusEchoHandler)
	nexusClient, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL: getDispatchByNsAndTqURL(env.HttpAPIAddress(), env.Namespace().String(), taskQueue),
		Service: "test-service",
	})
	s.NoError(err)

	requestHeaders := nexus.Header{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	_, err = nexusrpc.StartOperation(s.Context(), nexusClient, op, tv.Any().String(), nexus.StartOperationOptions{
		Header: requestHeaders,
	})
	s.NoError(err)
	s.NoError(<-pollerErrCh)
	s.requireExportedServerSpan(
		exporter,
		requestHeaders,
		strings.TrimPrefix(configs.DispatchNexusTaskByNamespaceAndTaskQueueAPIName, "/"),
		"io.temporal.frontend",
	)
}

func (s *NexusOTELSuite) requireExportedNexusHTTPSpanPairs(
	callerExporter *tracetest.InMemoryExporter,
	handlerExporter *tracetest.InMemoryExporter,
	expected int,
) {
	s.Await(func(s *NexusOTELSuite) {
		pairs := 0
		for _, serverSpan := range handlerExporter.GetSpans() {
			if serverSpan.Name != strings.TrimPrefix(configs.DispatchNexusTaskByEndpointAPIName, "/") ||
				serverSpan.SpanKind != oteltrace.SpanKindServer ||
				spanServiceName(serverSpan) != "io.temporal.frontend" {
				continue
			}
			for _, clientSpan := range callerExporter.GetSpans() {
				if clientSpan.SpanKind == oteltrace.SpanKindClient &&
					spanServiceName(clientSpan) == "io.temporal.history" &&
					clientSpan.SpanContext.TraceID() == serverSpan.SpanContext.TraceID() &&
					clientSpan.SpanContext.SpanID() == serverSpan.Parent.SpanID() {
					pairs++
				}
			}
		}
		s.Require().Equal(expected, pairs)
	}, 10*time.Second, 100*time.Millisecond)
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
	s.Await(func(s *NexusOTELSuite) {
		spans := exporter.GetSpans()
		for _, span := range spans {
			if span.SpanKind == oteltrace.SpanKindClient &&
				span.SpanContext.TraceID() == traceID &&
				span.SpanContext.SpanID() == spanID {
				exportedSpan = span
				return
			}
		}
		s.Require().Fail("matching client span not found", "exported spans: %v", spans)
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
	s.Await(func(s *NexusOTELSuite) {
		spans := exporter.GetSpans()
		for _, span := range spans {
			if span.Name == operation &&
				span.SpanKind == oteltrace.SpanKindServer &&
				span.SpanContext.TraceID() == traceID &&
				span.Parent.SpanID() == clientSpanID {
				exportedSpan = span
				return
			}
		}
		s.Require().Fail("matching server span not found", "exported spans: %v", spans)
	}, 10*time.Second, 100*time.Millisecond)
	s.requireSpanServiceName(exportedSpan, serviceName)
}

func (s *NexusOTELSuite) requireSpanServiceName(span tracetest.SpanStub, expected string) {
	s.Require().NotNil(span.Resource)
	serviceName, ok := span.Resource.Set().Value(semconv.ServiceNameKey)
	s.Require().True(ok)
	s.Require().Equal(expected, serviceName.AsString())
}

func spanServiceName(span tracetest.SpanStub) string {
	if span.Resource == nil {
		return ""
	}
	serviceName, ok := span.Resource.Set().Value(semconv.ServiceNameKey)
	if !ok {
		return ""
	}
	return serviceName.AsString()
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
