using System;
using System.IO;
using System.IO.Compression;

namespace GZipTest
{
    internal class GZipCompressor : ParallelFileProcessor
    {
        private static readonly int CompressorThreadCount = (int)Math.Ceiling(Environment.ProcessorCount * 1.5);

        internal GZipCompressor() : base(GetFreeFileToProcessBlockPool(), GetFreeProcessDataToFileQueueBlockPool(), CompressorThreadCount)
        {
        }

        protected override void ProcessDataBuffer(FileBlock fileBlockToProcess, FileBlock freeFileBlock)
        {
            using (var memoryStream = new MemoryStream(freeFileBlock.Buffer, 0, GZipConstants.MaxCompressedBufferSize))
            {
                using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Compress, true))
                {
                    gZipStream.Write(fileBlockToProcess.Buffer, 0, fileBlockToProcess.BufferSize);
                }
                freeFileBlock.BufferSize = (int)memoryStream.Position;
            }
        }

        protected override int ReadSourceFile(FileStream fileStream, FileBlock freeFileBlock)
        {
            freeFileBlock.BufferSize = fileStream.Read(freeFileBlock.Buffer, 0, GZipConstants.BufferSize);
            return freeFileBlock.BufferSize;
        }

        protected override void WriteDestinationFile(FileStream fileStream, FileBlock fileBlockToWrite)
        {
            fileStream.Write(BitConverter.GetBytes(fileBlockToWrite.BufferSize), 0, GZipConstants.BlockSizeSize);
            fileStream.Write(fileBlockToWrite.Buffer, 0, fileBlockToWrite.BufferSize);
        }

        private static FileBlock[] GetFreeFileToProcessBlockPool() => CreateBlocks(GZipConstants.BufferSize, CompressorThreadCount);

        private static FileBlock[] GetFreeProcessDataToFileQueueBlockPool() => CreateBlocks(GZipConstants.MaxCompressedBufferSize, CompressorThreadCount);
    }
}