// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;

namespace AWS.Messaging.Serialization.Helpers
{
    internal static class Utf8JsonReaderHelper
    {
        public static ReadOnlyMemory<byte> UnescapeValue(ref Utf8JsonReader reader, ArrayPoolScope arrayPoolScope)
        {
            if (!reader.ValueIsEscaped)
            {
                var span = reader.ValueSpan;
                var buffer = arrayPoolScope.GetBuffer(span.Length);
                span.CopyTo(buffer);
                return new ReadOnlyMemory<byte>(buffer, 0, span.Length);
            }

            var bytes = arrayPoolScope.GetBuffer(reader.ValueSpan.Length);
            var size = reader.CopyString(bytes);

            return new ReadOnlyMemory<byte>(bytes, 0, size);
        }
    }
}
