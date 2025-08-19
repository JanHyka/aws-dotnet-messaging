// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Configuration;
using AWS.Messaging.Serialization.Helpers;
using AWS.Messaging.Serialization.Parsers;
using AWS.Messaging.Services;
using Microsoft.Extensions.Logging;

namespace AWS.Messaging.Serialization;

/// <summary>
/// The performance based implementation of <see cref="IEnvelopeSerializer"/> used by the framework.
/// </summary>
internal class EnvelopeSerializerUtf8Json : IEnvelopeSerializer
{
    private Uri? MessageSource { get; set; }
    private const string CLOUD_EVENT_SPEC_VERSION = "1.0";

    // Pre-encoded property names to avoid repeated encoding and allocations
    private static readonly JsonEncodedText s_idProp = JsonEncodedText.Encode("id");
    private static readonly JsonEncodedText s_sourceProp = JsonEncodedText.Encode("source");
    private static readonly JsonEncodedText s_specVersionProp = JsonEncodedText.Encode("specversion");
    private static readonly JsonEncodedText s_typeProp = JsonEncodedText.Encode("type");
    private static readonly JsonEncodedText s_timeProp = JsonEncodedText.Encode("time");
    private static readonly JsonEncodedText s_dataContentTypeProp = JsonEncodedText.Encode("datacontenttype");
    private static readonly JsonEncodedText s_dataProp = JsonEncodedText.Encode("data");

    private readonly IMessageConfiguration _messageConfiguration;
    private readonly IMessageSerializer _messageSerializer;
    private readonly IDateTimeHandler _dateTimeHandler;
    private readonly IMessageIdGenerator _messageIdGenerator;
    private readonly IMessageSourceHandler _messageSourceHandler;
    private readonly ILogger<EnvelopeSerializer> _logger;

    private readonly IMessageSerializerUtf8Json? _messageSerializerUtf8Json;

    // Reader-based parsers (UTF-8 path). Order matters: fallback SQS must be last.
    private static readonly IMessageParserUtf8[] s_utf8Parsers = new IMessageParserUtf8[]
    {
        new SNSMessageParserUtf8(),
        new EventBridgeMessageParserUtf8(),
        new SQSMessageParserUtf8()
    };

    public EnvelopeSerializerUtf8Json(
        ILogger<EnvelopeSerializer> logger,
        IMessageConfiguration messageConfiguration,
        IMessageSerializer messageSerializer,
        IDateTimeHandler dateTimeHandler,
        IMessageIdGenerator messageIdGenerator,
        IMessageSourceHandler messageSourceHandler)
    {
        _logger = logger;
        _messageConfiguration = messageConfiguration;
        _messageSerializer = messageSerializer;
        _dateTimeHandler = dateTimeHandler;
        _messageIdGenerator = messageIdGenerator;
        _messageSourceHandler = messageSourceHandler;

        _messageSerializerUtf8Json = messageSerializer as IMessageSerializerUtf8Json;
    }

    /// <inheritdoc/>
    public async ValueTask<MessageEnvelope<T>> CreateEnvelopeAsync<T>(T message)
    {
        var messageId = await _messageIdGenerator.GenerateIdAsync();
        var timeStamp = _dateTimeHandler.GetUtcNow();

        var publisherMapping = _messageConfiguration.GetPublisherMapping(typeof(T));
        if (publisherMapping is null)
        {
            _logger.LogError("Failed to create a message envelope because a valid publisher mapping for message type '{MessageType}' does not exist.", typeof(T));
            throw new FailedToCreateMessageEnvelopeException($"Failed to create a message envelope because a valid publisher mapping for message type '{typeof(T)}' does not exist.");
        }

        if (MessageSource is null)
        {
            MessageSource = await _messageSourceHandler.ComputeMessageSource();
        }

        return new MessageEnvelope<T>
        {
            Id = messageId,
            Source = MessageSource,
            Version = CLOUD_EVENT_SPEC_VERSION,
            MessageTypeIdentifier = publisherMapping.MessageTypeIdentifier,
            TimeStamp = timeStamp,
            Message = message
        };
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access otherwise can break functionality when trimming application code", Justification = "<Pending>")]
    public async ValueTask<string> SerializeAsync<T>(MessageEnvelope<T> envelope)
    {
        try
        {
            await InvokePreSerializationCallback(envelope);
            T message = envelope.Message ?? throw new ArgumentNullException("The underlying application message cannot be null");

            using var buffer = new RentArrayBufferWriter(cleanRentedBuffers: _messageConfiguration.SerializationOptions.CleanRentedBuffers);
            using var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions
            {
                // We control the JSON shape here, so skip validation for performance
                SkipValidation = true
            });

            writer.WriteStartObject();

            writer.WriteString(s_idProp, envelope.Id);
            writer.WriteString(s_sourceProp, envelope.Source?.OriginalString);
            writer.WriteString(s_specVersionProp, envelope.Version);
            writer.WriteString(s_typeProp, envelope.MessageTypeIdentifier);
            writer.WriteString(s_timeProp, envelope.TimeStamp);

            if (_messageSerializerUtf8Json is not null)
            {
                writer.WriteString(s_dataContentTypeProp, _messageSerializerUtf8Json.ContentType);
                writer.WritePropertyName(s_dataProp);
                _messageSerializerUtf8Json.SerializeToBuffer(writer, message);
            }
            else
            {
                var response = _messageSerializer.Serialize(message);
                writer.WriteString(s_dataContentTypeProp, response.ContentType);
                writer.WritePropertyName(s_dataProp);
                if (IsJsonContentType(response.ContentType))
                {
                    writer.WriteRawValue(response.Data, skipInputValidation: true);
                }
                else
                {
                    writer.WriteStringValue(response.Data);
                }
            }

            // Write metadata as top-level properties
            foreach (var kvp in envelope.Metadata)
            {
                if (kvp.Key is not null &&
                    kvp.Value.ValueKind != JsonValueKind.Undefined &&
                    kvp.Value.ValueKind != JsonValueKind.Null &&
                    !s_knownEnvelopeProperties.Contains(kvp.Key))
                {
                    writer.WritePropertyName(kvp.Key);
                    kvp.Value.WriteTo(writer);
                }
            }

            writer.WriteEndObject();
            writer.Flush();

            var jsonString = Encoding.UTF8.GetString(buffer.WrittenSpan);
            var serializedMessage = await InvokePostSerializationCallback(jsonString);

            if (_messageConfiguration.LogMessageContent)
            {
                _logger.LogTrace("Serialized the MessageEnvelope object as the following raw string:\n{SerializedMessage}", serializedMessage);
            }
            else
            {
                _logger.LogTrace("Serialized the MessageEnvelope object to a raw string");
            }
            return serializedMessage;
        }
        catch (JsonException) when (!_messageConfiguration.LogMessageContent)
        {
            _logger.LogError("Failed to serialize the MessageEnvelope into a raw string");
            throw new FailedToSerializeMessageEnvelopeException("Failed to serialize the MessageEnvelope into a raw string");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to serialize the MessageEnvelope into a raw string");
            throw new FailedToSerializeMessageEnvelopeException("Failed to serialize the MessageEnvelope into a raw string", ex);
        }
    }

    /// <inheritdoc/>
    public async ValueTask<ConvertToEnvelopeResult> ConvertToEnvelopeAsync(Message sqsMessage)
    {
        try
        {
            using var arrayPoolScope = new ArrayPoolScope(_messageConfiguration.SerializationOptions.CleanRentedBuffers);

            // Get the raw envelope JSON and metadata from the appropriate wrapper (SNS/EventBridge/SQS) using UTF-8 path
            var (envelopeBytes, metadata) = await ParseOuterWrapperUtf8Async(sqsMessage, arrayPoolScope);

            // Create and populate the envelope with the correct type
            var (envelope, subscriberMapping) = DeserializeEnvelopeUtf8(envelopeBytes, arrayPoolScope);

            // Add metadata from outer wrapper
            envelope.SQSMetadata = metadata.SQSMetadata;
            envelope.SNSMetadata = metadata.SNSMetadata;
            envelope.EventBridgeMetadata = metadata.EventBridgeMetadata;

            await InvokePostDeserializationCallback(envelope);
            return new ConvertToEnvelopeResult(envelope, subscriberMapping);
        }
        catch (JsonException) when (!_messageConfiguration.LogMessageContent)
        {
            _logger.LogError("Failed to create a {MessageEnvelopeName}", nameof(MessageEnvelope));
            throw new FailedToCreateMessageEnvelopeException($"Failed to create {nameof(MessageEnvelope)}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create a {MessageEnvelopeName}", nameof(MessageEnvelope));
            throw new FailedToCreateMessageEnvelopeException($"Failed to create {nameof(MessageEnvelope)}", ex);
        }
    }

    // New UTF-8 reader-based outer wrapper parsing. Returns inner payload bytes and metadata.
    private async Task<(ReadOnlyMemory<byte> MessageBody, MessageMetadata Metadata)> ParseOuterWrapperUtf8Async(Message sqsMessage, ArrayPoolScope pool)
    {
        var body = await InvokePreDeserializationCallback(sqsMessage.Body);
        // Use a single backing array to allow zero-copy slicing in parsers
        // Avoid double pass over input: allocate using worst-case UTF-8 size, then encode once.
        var maxBytesNeeded = Encoding.UTF8.GetMaxByteCount(body.Length);
        var utf8 = pool.GetBuffer(maxBytesNeeded);
        var written = Encoding.UTF8.GetBytes(body.AsSpan(), utf8);
        var mem = new ReadOnlyMemory<byte>(utf8, 0, written);

        // Use parser QuickMatch to pick a likely parser first
        foreach (var parser in s_utf8Parsers)
        {
            if (parser.QuickMatch(mem.Span) && parser.TryParse(mem, sqsMessage, pool, out var inner, out var metadata))
            {
                return (inner, metadata);
            }
        }

        // Safety net: try all parsers in order (should not be needed)
        foreach (var parser in s_utf8Parsers)
        {
            if (parser.TryParse(mem, sqsMessage, pool, out var inner, out var metadata))
                return (inner, metadata);
        }

        return (mem, new MessageMetadata());
    }

    private bool IsJsonContentType(string? dataContentType)
    {
        if (string.IsNullOrWhiteSpace(dataContentType))
        {
            // If dataContentType is not specified, it should be treated as "application/json"
            return true;
        }

        ReadOnlySpan<char> contentType = dataContentType.AsSpan().Trim();

        // Remove parameters (anything after ';')
        int semicolonIndex = contentType.IndexOf(';');
        if (semicolonIndex >= 0)
            contentType = contentType.Slice(0, semicolonIndex).Trim();

        // Check "application/json" (case-insensitive)
        if (contentType.Equals("application/json", StringComparison.OrdinalIgnoreCase))
            return true;

        // Find the '/' separator
        int slashIndex = contentType.IndexOf('/');
        if (slashIndex < 0
            || slashIndex == contentType.Length - 1
            || slashIndex != contentType.LastIndexOf('/'))
        {
            // If there are multiple slashes, ends with a slash or there are no slashes at all, it's not a valid content type
            return false;
        }

        ReadOnlySpan<char> subtype = contentType.Slice(slashIndex + 1);

        // Check if the media subtype is "json" or ends with "+json"
        return subtype.Equals("json", StringComparison.OrdinalIgnoreCase)
            || subtype.EndsWith("+json", StringComparison.OrdinalIgnoreCase);
    }

    private static readonly FrozenSet<string> s_knownEnvelopeProperties = new HashSet<string> {
        "id",
        "source",
        "specversion",
        "type",
        "time",
        "datacontenttype",
        "data"
    }.ToFrozenSet();

    // New UTF-8 reader-based inner envelope deserialization.
    private (MessageEnvelope Envelope, SubscriberMapping Mapping) DeserializeEnvelopeUtf8(ReadOnlyMemory<byte> envelopeUtf8, ArrayPoolScope pool)
    {
        string? id = null;
        string? sourceStr = null;
        string? version = null;
        string? typeId = null;
        DateTimeOffset? timeStamp = null;
        string? dataContentType = null;
        ReadOnlyMemory<byte> dataJsonBytes = default; // holds JSON value bytes or unescaped string bytes
        bool dataIsJson = false; // capture once at parse time to avoid re-evaluating content type and handle out-of-order properties
        Dictionary<string, JsonElement>? metadataTemp = null;

        var reader = new Utf8JsonReader(envelopeUtf8.Span, isFinalBlock: true, state: default);
        if (reader.TokenType == JsonTokenType.None && !reader.Read())
            throw new InvalidDataException("Invalid JSON for MessageEnvelope");
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException("MessageEnvelope JSON must start with object");

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                reader.Skip();
                continue;
            }

            var name = reader.GetString();
            if (!reader.Read()) break;

            switch (name)
            {
                case "id":
                    id = reader.GetString();
                    break;
                case "source":
                    sourceStr = reader.GetString();
                    break;
                case "specversion":
                    version = reader.GetString();
                    break;
                case "type":
                    typeId = reader.GetString();
                    break;
                case "time":
                    timeStamp = reader.GetDateTimeOffset();
                    break;
                case "datacontenttype":
                    dataContentType = reader.GetString();
                    dataIsJson = IsJsonContentType(dataContentType);
                    break;
                case "data":
                    
                    if (dataIsJson)
                    {
                        // Capture exact JSON bytes of the value without allocation using the same backing memory
                        var start = (int)reader.TokenStartIndex;
                        reader.Skip();
                        var end = (int)reader.BytesConsumed;
                        dataJsonBytes = envelopeUtf8.Slice(start, end - start);
                    }
                    else
                    {
                        // Non-JSON content types must be JSON string tokens; unescape into pooled UTF-8 bytes.
                        dataJsonBytes = Utf8JsonReaderHelper.UnescapeValue(ref reader, pool);
                    }
                    break;
                default:
                    if (!s_knownEnvelopeProperties.Contains(name!))
                    {
                        metadataTemp ??= new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                        using var doc = JsonDocument.ParseValue(ref reader);
                        metadataTemp[name!] = doc.RootElement.Clone();
                    }
                    else
                    {
                        reader.Skip();
                    }
                    break;
            }
        }

        if (string.IsNullOrEmpty(typeId))
            throw new InvalidDataException("Message type identifier not found in envelope");

        var subscriberMapping = GetAndValidateSubscriberMapping(typeId);
        var envelope = subscriberMapping.MessageEnvelopeFactory.Invoke();

        try
        {
            envelope.Id = id ?? throw new InvalidDataException("Required property 'id' is missing");
            envelope.Source = sourceStr is not null ? new Uri(sourceStr, UriKind.RelativeOrAbsolute) : throw new InvalidDataException("Required property 'source' is missing");
            envelope.Version = version ?? throw new InvalidDataException("Required property 'specversion' is missing");
            envelope.MessageTypeIdentifier = typeId;
            envelope.TimeStamp = timeStamp ?? throw new InvalidDataException("Required property 'time' is missing or invalid");
            envelope.DataContentType = dataContentType;

            if (metadataTemp is not null)
            {
                foreach (var kvp in metadataTemp)
                {
                    envelope.Metadata[kvp.Key] = kvp.Value;
                }
            }

            // Deserialize the inner message
            object message;
            if (dataIsJson && _messageSerializerUtf8Json is not null)
            {
                message = _messageSerializerUtf8Json!.Deserialize(dataJsonBytes.Span, subscriberMapping.MessageType);
            }
            else
            {
                // Fallback to string-based deserializer (also used for non-JSON content types)
                var text = Encoding.UTF8.GetString(dataJsonBytes.Span);
                message = _messageSerializer.Deserialize(text, subscriberMapping.MessageType);
            }

            envelope.SetMessage(message);
            return (envelope, subscriberMapping);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize or validate MessageEnvelope");
            throw new InvalidDataException("MessageEnvelope instance is not valid", ex);
        }
    }

    private SubscriberMapping GetAndValidateSubscriberMapping(string messageTypeIdentifier)
    {
        var subscriberMapping = _messageConfiguration.GetSubscriberMapping(messageTypeIdentifier);
        if (subscriberMapping is null)
        {
            var availableMappings = string.Join(", ",
                _messageConfiguration.SubscriberMappings.Select(m => m.MessageTypeIdentifier));

            _logger.LogError(
                "'{MessageTypeIdentifier}' is not a valid subscriber mapping. Available mappings: {AvailableMappings}",
                messageTypeIdentifier,
                string.IsNullOrEmpty(availableMappings) ? "none" : availableMappings);

            throw new InvalidDataException(
                $"'{messageTypeIdentifier}' is not a valid subscriber mapping. " +
                $"Available mappings: {(string.IsNullOrEmpty(availableMappings) ? "none" : availableMappings)}");
        }
        return subscriberMapping;
    }

    private async ValueTask InvokePreSerializationCallback(MessageEnvelope messageEnvelope)
    {
        foreach (var serializationCallback in _messageConfiguration.SerializationCallbacks)
        {
            await serializationCallback.PreSerializationAsync(messageEnvelope);
        }
    }

    private async ValueTask<string> InvokePostSerializationCallback(string message)
    {
        foreach (var serializationCallback in _messageConfiguration.SerializationCallbacks)
        {
            message = await serializationCallback.PostSerializationAsync(message);
        }
        return message;
    }

    private async ValueTask<string> InvokePreDeserializationCallback(string message)
    {
        foreach (var serializationCallback in _messageConfiguration.SerializationCallbacks)
        {
            message = await serializationCallback.PreDeserializationAsync(message);
        }
        return message;
    }

    private async ValueTask InvokePostDeserializationCallback(MessageEnvelope messageEnvelope)
    {
        foreach (var serializationCallback in _messageConfiguration.SerializationCallbacks)
        {
            await serializationCallback.PostDeserializationAsync(messageEnvelope);
        }
    }
}
