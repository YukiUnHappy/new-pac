using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;

namespace DDB
{
    class Program
    {
        private static WebClient _wc;
        private static HttpClient _client;

        private static void Main(string[] args)
        {
            var l = new List<string>
            {
                "https://d21agqkwgk4jud.cloudfront.net/MW-Metadata/MW/MWM-MW-9-0001.tar.gz",
                "https://d21agqkwgk4jud.cloudfront.net/MW-Metadata/MW/MWM-MW-9-0002.tar.gz",
                "https://d21agqkwgk4jud.cloudfront.net/MW-Metadata/SP/MWM-SP-1-0001.tar.gz",
                "https://d21agqkwgk4jud.cloudfront.net/MW-Metadata/DM/MWM-DM-1-0001.tar.gz"
            };


            var handler = new HttpClientHandler();
#if DEBUG
            handler.Proxy = new WebProxy("127.0.0.1", 1083);
#endif
            _client = new HttpClient(handler);

            _wc = new WebClient();
#if DEBUG
            _wc.Proxy = new WebProxy("127.0.0.1", 1083);
#endif

#if DEBUG
            var config = new AmazonDynamoDBConfig
            {
                RegionEndpoint = RegionEndpoint.APNortheast1,
                ProxyHost = "127.0.0.1",
                ProxyPort = 1081
            };
#else
            var client = new AmazonDynamoDBClient();
#endif
            var table = Table.LoadTable(client, "MWDatabase");

            Console.WriteLine("Start");

            var r = new Regex("{\"content_id\":\"(.+?)\"");

            DownloadCache();

            if (!Directory.Exists("cache"))
                Directory.CreateDirectory("cache");

            foreach (var s in l)
            {
                var k = Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(s));

                var data = _wc.DownloadData(s);
                using var ms = new MemoryStream(data);

                var dir = $"ctx/{k}";
                var cDir = $"cache/{k}";

                ExtractTarGz(ms, dir);

                Console.WriteLine(k);

                var dic = new Dictionary<string, string>();

                if (File.Exists($"{cDir}/contents.json"))
                {
                    foreach (var line in File.ReadAllLines($"{cDir}/contents.json"))
                    {
                        var cid = r.Match(line).Groups[1].Value;
                        dic.Add(cid, Hash(line));
                    }
                }

                var list = new List<string>();

                foreach (var line in File.ReadAllLines($"{dir}/contents.json"))
                {
                    var cid = r.Match(line).Groups[1].Value;
                    var flag = dic.TryGetValue(cid, out var hash);

                    if (!flag || hash != Hash(line))
                        list.Add(line);
                }

                for (var i = 0; i < list.Count; i += 2)
                {
                    var batchWrite = table.CreateBatchWrite();

                    for (var j = i; j < i + 2; j++)
                    {
                        if (j >= list.Count)
                            break;
                        var doc = Document.FromJson(list[j]);
                        batchWrite.AddDocumentToPut(doc);
                    }

                    batchWrite.ExecuteAsync().GetAwaiter().GetResult();
                }

                if (dic.Count != 0)
                    Directory.Delete(cDir, true);
                Directory.Move(dir, cDir);

                Console.WriteLine($"{list.Count}/{dic.Count}");
            }

            UploadCache();

            Console.WriteLine("Finish");
        }

        private static readonly Regex regex = new Regex("\"files\":\\[{\"name\":\"(.+?)\",\"url\":\"(.+?)\"");

        private static string Upload(string path)
        {
            using var formData = new MultipartFormDataContent();

            formData.Headers.Add("filelength", "");
            _client.DefaultRequestHeaders.Add("age", (7 * 24).ToString());

            formData.Add(new ByteArrayContent(File.ReadAllBytes(path)), "files[]",
                "file" + Path.GetExtension(path));

            var response = _client.PostAsync("https://safe.fiery.me/api/upload", formData).GetAwaiter().GetResult();

            // ensure the request was a success
            if (!response.IsSuccessStatusCode)
                return Upload(path);

            var result = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();

            var m = regex.Match(result);

            var name = m.Groups[1].Value;
            var url = m.Groups[2].Value;

            return name;
        }

        private static readonly string CKey = Environment.GetEnvironmentVariable("KVKEY") ?? "tcb";
        private const string Kv = "https://keyvalue.immanuel.co/api/KeyVal/COM/nk4z9vqp";

        private static void UploadCache()
        {
            ZipFile.CreateFromDirectory("cache", "tmp.zip");

            var name = Upload("tmp.zip");

            _client.PostAsync($"{Kv.Replace("COM", "UpdateValue")}/{CKey}/{Path.GetFileNameWithoutExtension(name)}",
                null).GetAwaiter().GetResult();
        }

        private static void DownloadCache()
        {
            var k = _client.GetAsync($"{Kv.Replace("COM", "GetValue")}/{CKey}").GetAwaiter().GetResult().Content
                .ReadAsStringAsync().GetAwaiter().GetResult()[1..^1];

            if (string.IsNullOrWhiteSpace(k) || k == "n")
                return;

            var f = $"https://i.fiery.me/{k}.zip";

            _wc.DownloadFile(f, "cache.zip");

            ZipFile.ExtractToDirectory("cache.zip", "cache");
        }

        public static string Hash(string t)
        {
            using var sha1 = new SHA1CryptoServiceProvider();
            var bytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(t));
            var sBuilder = new StringBuilder();
            foreach (var b in bytes)
                sBuilder.Append(b.ToString("x2"));
            return sBuilder.ToString();
        }

        public static void ExtractTarGz(string filename, string outputDir)
        {
            using var stream = File.OpenRead(filename);
            ExtractTarGz(stream, outputDir);
        }

        public static void ExtractTarGz(Stream stream, string outputDir)
        {
            // A GZipStream is not seekable, so copy it first to a MemoryStream
            using var gzip = new GZipStream(stream, CompressionMode.Decompress);
            const int chunk = 4096;
            using var memStr = new MemoryStream();
            int read;
            var buffer = new byte[chunk];
            do
            {
                read = gzip.Read(buffer, 0, chunk);
                memStr.Write(buffer, 0, read);
            } while (read == chunk);

            memStr.Seek(0, SeekOrigin.Begin);
            ExtractTar(memStr, outputDir);
        }

        public static void ExtractTar(string filename, string outputDir)
        {
            using var stream = File.OpenRead(filename);
            ExtractTar(stream, outputDir);
        }

        public static void ExtractTar(Stream stream, string outputDir)
        {
            var buffer = new byte[100];

            while (true)
            {
                stream.Read(buffer, 0, 100);
                var name = Encoding.ASCII.GetString(buffer).Trim('\0');
                if (string.IsNullOrEmpty(name))
                    break;
                stream.Seek(24, SeekOrigin.Current);
                stream.Read(buffer, 0, 12);

                var size = Convert.ToInt64(Encoding.UTF8.GetString(buffer, 0, 12).Trim('\0').Trim(), 8);

                stream.Seek(376L, SeekOrigin.Current);

                var output = Path.Combine(outputDir, name);
                if (!Directory.Exists(Path.GetDirectoryName(output)))
                    Directory.CreateDirectory(Path.GetDirectoryName(output));
                if (!name.EndsWith("/"))
                {
                    using var str = File.Open(output, FileMode.Create, FileAccess.Write);
                    var buf = new byte[size];
                    stream.Read(buf, 0, buf.Length);
                    str.Write(buf, 0, buf.Length);
                }

                var pos = stream.Position;

                var offset = 512 - (pos % 512);
                if (offset == 512)
                    offset = 0;

                stream.Seek(offset, SeekOrigin.Current);
            }
        }
    }
}
