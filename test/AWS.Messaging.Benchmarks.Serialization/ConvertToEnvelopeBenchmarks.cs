using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using AWS.Messaging.Serialization;
using AWS.Messaging.Services;
using AWS.Messaging.UnitTests.Models;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace AWS.Messaging.Benchmarks.Serialization;

public enum MessageKind
{
    SqsRaw,
    SnsString,
    SnsObject,
    EbString,
    EbObject
}

public enum SerializerKind
{
    Standard,
    StandardWithJsonContext,
    JsonWriter,
    JsonWriterWithJsonContext,
    JsonWriterWithJsonContextUnsafe
}

[MemoryDiagnoser]
public class ConvertToEnvelopeBenchmarks
{
    private IEnvelopeSerializer _standardSerializer = null!;
    private IEnvelopeSerializer _standardSerializerWithJsonContext = null!;
    private IEnvelopeSerializer _jsonWriterSerializer = null!;
    private IEnvelopeSerializer _jsonWriterSerializerWithJsonContext = null!;
    private IEnvelopeSerializer _jsonWriterSerializerWithJsonContextUnsafe = null!;

    private Message _sqsRawMessage = null!;
    private Message _snsStringWrapped = null!;
    private Message _snsObjectWrapped = null!;
    private Message _ebStringWrapped = null!;
    private Message _ebObjectWrapped = null!;

    private Mock<IDateTimeHandler> _mockDateTimeHandler = null!;

    // Ordering of Params controls grouping in summary output: MessageKind first, then ItemCount, then SerializerKind
    [Params(MessageKind.SqsRaw, MessageKind.SnsString, MessageKind.SnsObject, MessageKind.EbString, MessageKind.EbObject)]
    public MessageKind Kind { get; set; }

    [Params(1, 10, 100, 1000)]
    public int ItemCount { get; set; }

    [Params(SerializerKind.Standard, SerializerKind.StandardWithJsonContext, SerializerKind.JsonWriter, SerializerKind.JsonWriterWithJsonContext, SerializerKind.JsonWriterWithJsonContextUnsafe)]
    public SerializerKind Serializer { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _mockDateTimeHandler = new Mock<IDateTimeHandler>();
        var testDate = new DateTime(2023, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        _mockDateTimeHandler.Setup(x => x.GetUtcNow()).Returns(testDate);

        CreateStandardSerializer();
        CreateStandardSerializerWithJsonContext();
        CreateJsonWriterSerializerWithJsonContext();
        CreateJsonWriterSerializerWithJsonContextUnsafe();
        CreateJsonWriterSerializer();

        // build test payload
        var items = new List<AddressInfo>(ItemCount);
        for (var i = 0; i < ItemCount; i++)
        {
            items.Add(new AddressInfo
            {
                Street = $"Street {i}",
                Unit = i,
                ZipCode = $"{10000 + i}"
            });
        }

        var message = new AddressInfoListEnvelope { Items = items };
        var envelope = new MessageEnvelope<AddressInfoListEnvelope>
        {
            Id = "id-123",
            Source = new Uri("/backend/service", UriKind.Relative),
            Version = "1.0",
            MessageTypeIdentifier = "addressInfoList",
            TimeStamp = DateTimeOffset.UtcNow,
            Message = message
        };

        // Use the standard serializer to generate a canonical CloudEvents envelope JSON body
        var cloudEventJson = await _standardSerializer.SerializeAsync(envelope);

        // Prepare SQS messages with different wrappers
        _sqsRawMessage = new Message { Body = cloudEventJson };

        _snsStringWrapped = new Message
        {
            Body = "{" +
                   "\"Type\":\"Notification\"," +
                   "\"MessageId\":\"mid-1\"," +
                   "\"TopicArn\":\"arn:aws:sns:us-east-1:123456789012:topic\"," +
                   "\"Timestamp\":\"2024-01-01T00:00:00Z\"," +
                   "\"Message\":" + System.Text.Json.JsonSerializer.Serialize(cloudEventJson) + // ensure proper escaping
                   "}"
        };

        _snsObjectWrapped = new Message
        {
            Body = "{" +
                   "\"Type\":\"Notification\"," +
                   "\"MessageId\":\"mid-1\"," +
                   "\"TopicArn\":\"arn:aws:sns:us-east-1:123456789012:topic\"," +
                   "\"Timestamp\":\"2024-01-01T00:00:00Z\"," +
                   "\"Message\":" + cloudEventJson +
                   "}"
        };

        _ebStringWrapped = new Message
        {
            Body = "{" +
                   "\"id\":\"eid-1\"," +
                   "\"detail-type\":\"addressInfoList\"," +
                   "\"source\":\"/aws/messaging\"," +
                   "\"time\":\"2024-01-01T00:00:00Z\"," +
                   "\"account\":\"123456789012\"," +
                   "\"region\":\"us-east-1\"," +
                   "\"detail\":" + System.Text.Json.JsonSerializer.Serialize(cloudEventJson) +
                   "}"
        };

        _ebObjectWrapped = new Message
        {
            Body = "{" +
                   "\"id\":\"eid-1\"," +
                   "\"detail-type\":\"addressInfoList\"," +
                   "\"source\":\"/aws/messaging\"," +
                   "\"time\":\"2024-01-01T00:00:00Z\"," +
                   "\"account\":\"123456789012\"," +
                   "\"region\":\"us-east-1\"," +
                   "\"detail\":" + cloudEventJson +
                   "}"
        };
    }

    private void CreateStandardSerializer()
    {
        var _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging();
        _serviceCollection.AddAWSMessageBus(builder =>
        {
            builder.AddSQSPublisher<AddressInfoListEnvelope>("sqsQueueUrl", "addressInfoList");
            builder.AddMessageHandler<AddressInfoListHandler, AddressInfoListEnvelope>("addressInfoList");
            builder.AddMessageSource("/aws/messaging");
        });

        _serviceCollection.Replace(new ServiceDescriptor(typeof(IDateTimeHandler), _mockDateTimeHandler.Object));
        _standardSerializer = _serviceCollection.BuildServiceProvider().GetRequiredService<IEnvelopeSerializer>();
    }

    private void CreateStandardSerializerWithJsonContext()
    {
        var _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging();
        _serviceCollection.AddAWSMessageBus(new AddressInfoListEnvelopeSerializerContext(), builder =>
        {
            builder.AddSQSPublisher<AddressInfoListEnvelope>("sqsQueueUrl", "addressInfoList");
            builder.AddMessageHandler<AddressInfoListHandler, AddressInfoListEnvelope>("addressInfoList");
            builder.AddMessageSource("/aws/messaging");
        });
        _serviceCollection.Replace(new ServiceDescriptor(typeof(IDateTimeHandler), _mockDateTimeHandler.Object));
        _standardSerializerWithJsonContext = _serviceCollection.BuildServiceProvider().GetRequiredService<IEnvelopeSerializer>();
    }

    private void CreateJsonWriterSerializerWithJsonContext()
    {
        var _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging();
        _serviceCollection.AddAWSMessageBus(new AddressInfoListEnvelopeSerializerContext(), builder =>
        {
            builder.AddSQSPublisher<AddressInfoListEnvelope>("sqsQueueUrl", "addressInfoList");
            builder.AddMessageHandler<AddressInfoListHandler, AddressInfoListEnvelope>("addressInfoList");
            builder.AddMessageSource("/aws/messaging");
            builder.EnableExperimentalFeatures();
        });
        _serviceCollection.Replace(new ServiceDescriptor(typeof(IDateTimeHandler), _mockDateTimeHandler.Object));
        _jsonWriterSerializerWithJsonContext = _serviceCollection.BuildServiceProvider().GetRequiredService<IEnvelopeSerializer>();
    }

    private void CreateJsonWriterSerializerWithJsonContextUnsafe()
    {
        var _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging();
        _serviceCollection.AddAWSMessageBus(new AddressInfoListEnvelopeSerializerContext(), builder =>
        {
            builder.AddSQSPublisher<AddressInfoListEnvelope>("sqsQueueUrl", "addressInfoList");
            builder.AddMessageHandler<AddressInfoListHandler, AddressInfoListEnvelope>("addressInfoList");
            builder.AddMessageSource("/aws/messaging");
            builder.EnableExperimentalFeatures();
            builder.ConfigureSerializationOptions(options =>
            {
                options.CleanRentedBuffers = false; // Disable cleaning rented buffers for performance
            });
        });
        _serviceCollection.Replace(new ServiceDescriptor(typeof(IDateTimeHandler), _mockDateTimeHandler.Object));
        _jsonWriterSerializerWithJsonContextUnsafe = _serviceCollection.BuildServiceProvider().GetRequiredService<IEnvelopeSerializer>();
    }

    private void CreateJsonWriterSerializer()
    {
        var _serviceCollection = new ServiceCollection();
        _serviceCollection.AddLogging();
        _serviceCollection.AddAWSMessageBus(builder =>
        {
            builder.AddSQSPublisher<AddressInfoListEnvelope>("sqsQueueUrl", "addressInfoList");
            builder.AddMessageHandler<AddressInfoListHandler, AddressInfoListEnvelope>("addressInfoList");
            builder.AddMessageSource("/aws/messaging");
            builder.EnableExperimentalFeatures();
        });
        _serviceCollection.Replace(new ServiceDescriptor(typeof(IDateTimeHandler), _mockDateTimeHandler.Object));
        _jsonWriterSerializer = _serviceCollection.BuildServiceProvider().GetRequiredService<IEnvelopeSerializer>();
    }

    [Benchmark]
    public async Task<ConvertToEnvelopeResult> Convert()
    {
        var serializer = GetSerializer(Serializer);
        var message = GetMessage(Kind);
        return await serializer.ConvertToEnvelopeAsync(message);
    }

    private IEnvelopeSerializer GetSerializer(SerializerKind kind) => kind switch
    {
        SerializerKind.Standard => _standardSerializer,
        SerializerKind.StandardWithJsonContext => _standardSerializerWithJsonContext,
        SerializerKind.JsonWriter => _jsonWriterSerializer,
        SerializerKind.JsonWriterWithJsonContext => _jsonWriterSerializerWithJsonContext,
        SerializerKind.JsonWriterWithJsonContextUnsafe => _jsonWriterSerializerWithJsonContextUnsafe,
        _ => _standardSerializer
    };

    private Message GetMessage(MessageKind kind) => kind switch
    {
        MessageKind.SqsRaw => _sqsRawMessage,
        MessageKind.SnsString => _snsStringWrapped,
        MessageKind.SnsObject => _snsObjectWrapped,
        MessageKind.EbString => _ebStringWrapped,
        MessageKind.EbObject => _ebObjectWrapped,
        _ => _sqsRawMessage
    };
}
