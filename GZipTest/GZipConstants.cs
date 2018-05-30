using System;

namespace GZipTest
{
    internal static class GZipConstants
    {
        internal const int BlockSizeSize = sizeof(int);

        internal const int BufferSize = 1024 * 1024 * 4;

        internal static readonly int MaxCompressedBufferSize = (int)Math.Ceiling(BufferSize * 2.1);
    }
}