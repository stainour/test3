using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace GZipTest
{
    internal abstract class ParallelFileProcessor
    {
        protected int _currentReadingFileBlockIndex;
        protected int _currentWritingFileBlockIndex;
        private readonly int ProcessorThreadsCount;
        private Exception _exception;
        private SimpleBlockingQueue<FileBlock> _fileToProcessQueue;
        private SimpleBlockingQueue<FileBlock> _freeFileToProcessQueue;
        private SimpleBlockingQueue<FileBlock> _freeProcessDataToFileQueue;
        private SimpleBlockingQueue<FileBlock> _processDataToFileQueue;
        private Dictionary<int, FileBlock> _skippedFileBlocks;

        protected ParallelFileProcessor(FileBlock[] freeFileToProcessQueue, FileBlock[] freeProcessDataToFileQueue, int processorCount)
        {
            ProcessorThreadsCount = processorCount;
            _processDataToFileQueue = new SimpleBlockingQueue<FileBlock>(ProcessorThreadsCount);
            _fileToProcessQueue = new SimpleBlockingQueue<FileBlock>(1);
            _freeFileToProcessQueue = new SimpleBlockingQueue<FileBlock>(freeFileToProcessQueue, ProcessorThreadsCount);
            _freeProcessDataToFileQueue = new SimpleBlockingQueue<FileBlock>(freeProcessDataToFileQueue, 1);
            _skippedFileBlocks = new Dictionary<int, FileBlock>();
        }

        ///<summary>Запуск обработки файла</summary>
        /// <param name="sourceFilePath">Путь к исходному файлу</param>
        /// <param name="destinationFilePath">Путь к выходному файлу</param>
        /// <exception cref="ArgumentNullException">
        /// </exception>
        /// <exception cref="DirectoryNotFoundException">
        /// </exception>
        /// <exception cref="FileFormatException">
        /// </exception>
        internal void Run(string sourceFilePath, string destinationFilePath)
        {
            if (sourceFilePath == null) throw new ArgumentNullException(nameof(sourceFilePath));
            if (destinationFilePath == null) throw new ArgumentNullException(nameof(destinationFilePath));

            _currentReadingFileBlockIndex = 0;
            _currentWritingFileBlockIndex = 0;
            _exception = null;
            _skippedFileBlocks.Clear();

            var readingThread = new Thread(ReadSourceFileTemplate)
            {
                Name = "Reader"
            };
            readingThread.Start(sourceFilePath);

            var writingThread = new Thread(WriteDestinationFileTemplate)
            {
                Name = "Writer"
            };
            writingThread.Start(destinationFilePath);

            var processingDataThreads = new Thread[ProcessorThreadsCount];

            for (int i = 0; i < ProcessorThreadsCount; i++)
            {
                processingDataThreads[i] = new Thread(ProcessDataBufferTemplate) { Name = $"Processor {i}" };
                processingDataThreads[i].Start();
            }

            readingThread.Join();

            for (int i = 0; i < ProcessorThreadsCount; i++)
            {
                processingDataThreads[i].Join();
            }

            writingThread.Join();

            if (_exception != null)
            {
                throw _exception;
            }
        }

        protected static FileBlock[] CreateBlocks(int blockSize, int blockCount)
        {
            if (blockSize <= 0) throw new ArgumentOutOfRangeException(nameof(blockSize));
            if (blockCount <= 0) throw new ArgumentOutOfRangeException(nameof(blockCount));
            var blocks = new FileBlock[blockCount];

            for (int i = 0; i < blockCount; i++)
            {
                blocks[i] = new FileBlock(new byte[blockSize]);
            }
            return blocks;
        }

        protected abstract void ProcessDataBuffer(FileBlock fileBlockToProcess, FileBlock freeFileBlock);

        protected void ProcessDataBufferTemplate()
        {
            try
            {
                while (_exception == null)
                {
                    var freeBlock = _freeProcessDataToFileQueue.Dequeue();

                    var blockToProcess = _fileToProcessQueue.Dequeue();

                    if (blockToProcess == default(FileBlock))
                    {
                        return;
                    }

                    freeBlock.Index = blockToProcess.Index;
                    ProcessDataBuffer(blockToProcess, freeBlock);

                    _freeFileToProcessQueue.Enqueue(blockToProcess);
                    _processDataToFileQueue.Enqueue(freeBlock);
                }
            }
            catch (Exception e)
            {
                _exception = e;
            }
            finally
            {
                _processDataToFileQueue.StopProducer();
                _freeFileToProcessQueue.StopProducer();
            }
        }

        protected abstract int ReadSourceFile(FileStream fileStream, FileBlock freeFileBlock);

        protected void ReadSourceFileTemplate(object sourceFilePath)
        {
            try
            {
                using (FileStream fileStream = File.OpenRead((string)sourceFilePath))
                {
                    while (_exception == null)
                    {
                        var freeBlock = _freeFileToProcessQueue.Dequeue();
                        freeBlock.Index = _currentReadingFileBlockIndex;
                        var bytesRead = ReadSourceFile(fileStream, freeBlock);
                        if (bytesRead == 0)
                            break;
                        _fileToProcessQueue.Enqueue(freeBlock);
                        ++_currentReadingFileBlockIndex;
                    }
                }
            }
            catch (Exception e)
            {
                _exception = e;
            }
            finally
            {
                _fileToProcessQueue.StopProducer();
            }
        }

        protected abstract void WriteDestinationFile(FileStream fileStream, FileBlock fileBlockToWrite);

        protected void WriteDestinationFileTemplate(object destinationFilePath)
        {
            try
            {
                using (FileStream fileStream = File.OpenWrite((string)destinationFilePath))
                {
                    while (_exception == null)
                    {
                        FileBlock fileBlockToWrite;
                        if (_skippedFileBlocks.TryGetValue(_currentWritingFileBlockIndex, out fileBlockToWrite))
                        {
                            _skippedFileBlocks.Remove(_currentWritingFileBlockIndex);
                        }
                        else
                        {
                            fileBlockToWrite = _processDataToFileQueue.Dequeue();
                            if (fileBlockToWrite == default(FileBlock))
                            {
                                if (_skippedFileBlocks.Count != 0)
                                {
                                    continue;
                                }
                                break;
                            }
                        }

                        if (fileBlockToWrite.Index != _currentWritingFileBlockIndex)
                        {
                            _skippedFileBlocks.Add(fileBlockToWrite.Index, fileBlockToWrite);
                            continue;
                        }

                        WriteDestinationFile(fileStream, fileBlockToWrite);
                        _currentWritingFileBlockIndex++;
                        _freeProcessDataToFileQueue.Enqueue(fileBlockToWrite);
                    }
                }
            }
            catch (Exception e)
            {
                _exception = e;
            }
        }

        protected class FileBlock
        {
            public readonly byte[] Buffer;
            public int BufferSize;
            public int Index;

            public FileBlock(byte[] buffer)
            {
                Buffer = buffer;
            }
        }
    }
}