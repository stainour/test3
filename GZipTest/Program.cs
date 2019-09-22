using System;

namespace GZipTest
{
    internal class Program
    {
        private static string _wrongArgumentsErrorMessage = "Правильное использование: compress [имя исходного файла] [имя архива] или decompress  [имя архива] [имя распакованного файла]";

        private static int HandleArgumentsError()
        {
            Console.WriteLine(_wrongArgumentsErrorMessage);
            Console.WriteLine("Для продолжения нажмите Enter");
            Console.ReadLine();
            return 1;
        }

        private static int Main(string[] args)
        {
            if (args.Length != 3)
            {
                return HandleArgumentsError();
            }
            var s = args[0].Trim();
            ParallelFileProcessor fileProcessor;
            if (string.Equals(s, "decompress", StringComparison.OrdinalIgnoreCase))
            {
                fileProcessor = new GZipDecompressor();
            }
            else if (string.Equals(s, "compress", StringComparison.OrdinalIgnoreCase))
            {
                fileProcessor = new GZipCompressor();
            }
            else
            {
                return HandleArgumentsError();
            }
            try
            {
                fileProcessor.Run(args[1], args[2]);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Произошла ошибка {e.Message}");
                Console.WriteLine("Для продолжения нажмите Enter");
                Console.ReadLine();
                return 1;
            }

            return 0;
        }
    }
}