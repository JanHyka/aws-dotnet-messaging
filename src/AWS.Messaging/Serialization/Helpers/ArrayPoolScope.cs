// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Buffers;

namespace AWS.Messaging.Serialization.Helpers
{
    internal sealed class ArrayPoolScope : IDisposable
    {
        private readonly List<byte[]> _buffers = new();
        private readonly bool _cleanRentedBuffers;

        private bool _isDisposed;

        public ArrayPoolScope(bool cleanRentedBuffers = true)
        {
            _cleanRentedBuffers = cleanRentedBuffers;
        }

        public byte[] GetBuffer(int minimumLength)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(minimumLength);
            _buffers.Add(buffer);
            return buffer;
        }

        private void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    foreach (var buffer in _buffers)
                    {
                        ArrayPool<byte>.Shared.Return(buffer, clearArray: _cleanRentedBuffers);
                    }

                    _buffers.Clear();
                }

                _isDisposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
