using System;
using System.IO;
using System.IO.Compression;

namespace GZipTest
{
    internal class GZipDecompressor : ParallelFileProcessor
    {
        private static readonly string _badArchiveMessage = "Архив поврежден!";
        private static readonly int DecompressorThreadCount = Environment.ProcessorCount * 2;
        private readonly byte[] _blockSizeBuffer = new byte[GZipConstants.BlockSizeSize];

        internal GZipDecompressor() : base(GetFreeFileToProcessBlockPool(), GetFreeProcessDataToFileQueueBlockPool(), DecompressorThreadCount)
        {
        }

        protected override void ProcessDataBuffer(FileBlock fileBlockToProcess, FileBlock freeFileBlock)
        {
            using (var memoryStream = new MemoryStream(fileBlockToProcess.Buffer, 0, fileBlockToProcess.BufferSize))
            {
                using (var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress, true))
                {
                    freeFileBlock.BufferSize = gZipStream.Read(freeFileBlock.Buffer, 0, GZipConstants.BufferSize);
                }
            }
        }

        protected override int ReadSourceFile(FileStream fileStream, FileBlock freeFileBlock)
        {
            int bytesRead;

            if ((bytesRead = fileStream.Read(_blockSizeBuffer, 0, GZipConstants.BlockSizeSize)) < GZipConstants.BlockSizeSize)
            {
                if (bytesRead == 0)
                {
                    return 0;
                }
                throw new FileFormatException(_badArchiveMessage);
            }

            freeFileBlock.BufferSize = BitConverter.ToInt32(_blockSizeBuffer, 0);
            if (fileStream.Read(freeFileBlock.Buffer, 0, freeFileBlock.BufferSize) < freeFileBlock.BufferSize)
            {
                throw new FileFormatException(_badArchiveMessage);
            }
            return freeFileBlock.BufferSize;
        }

        protected override void WriteDestinationFile(FileStream fileStream, FileBlock fileBlockToWrite)
        {
            fileStream.Write(fileBlockToWrite.Buffer, 0, fileBlockToWrite.BufferSize);
        }

        private static FileBlock[] GetFreeFileToProcessBlockPool() => CreateBlocks(GZipConstants.MaxCompressedBufferSize, DecompressorThreadCount);

        private static FileBlock[] GetFreeProcessDataToFileQueueBlockPool() => CreateBlocks(GZipConstants.BufferSize, DecompressorThreadCount);
    }
}