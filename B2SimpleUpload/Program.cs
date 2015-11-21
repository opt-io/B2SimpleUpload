/*
Quick and simple B2 file upload utility, done as an example.  Uses Json.Net and RestSharp.


Usage:

B2SimpleUpload <Account ID> <Application Key> <Bucket> <Path of file to upload>

opt.io, 2015
*/

namespace B2SimpleUpload
{
    using Newtonsoft.Json.Linq;
    using RestSharp;
    using RestSharp.Authenticators;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Security.Cryptography;

    class Program
    {
        private static readonly int uploadBuffer = 1024 * 32;

        static int Main(string[] args)
        {
            string accountID = args[0];
            string applicationKey = args[1];
            string bucketName = args[2];
            string fileToUpload = args[3];


            if (!File.Exists(fileToUpload))
            {
                Console.WriteLine("File to upload does not exist!");
                return 1;
            }

            // Get Auth Token
            Console.Write("Getting Auth Token... ");

            var client = new RestClient("https://api.backblaze.com")
            {
                Authenticator = new HttpBasicAuthenticator(accountID, applicationKey)
            };

            var request = new RestRequest("/b2api/v1/b2_authorize_account", Method.GET);

            var response = client.Execute(request);
            var content = response.Content;

            if (response.StatusCode != HttpStatusCode.OK)
            {
                Console.WriteLine(content);
                return 1;
            }


            var jAuth = JObject.Parse(content);
            var authorizationToken = (string)jAuth["authorizationToken"];
            var apiUrl = (string)jAuth["apiUrl"];

            Console.WriteLine("Done.");

            // Update the client with the updated API
            client = new RestClient(apiUrl);


            // Get Bucket ID
            Console.Write("Getting Bucket ID... ");

            request = new RestRequest("/b2api/v1/b2_list_buckets", Method.POST);
            request.RequestFormat = DataFormat.Json;
            request.AddHeader("Authorization", authorizationToken);
            request.AddBody(new { accountId = accountID });

            response = client.Execute(request);
            content = response.Content;

            if (response.StatusCode != HttpStatusCode.OK)
            {
                Console.WriteLine(content);
                return 1;
            }

            var jBuckets = JObject.Parse(content);

            var buckets = (JArray)jBuckets["buckets"];

            var bucketID = (string)buckets.FirstOrDefault(v => (string)v["bucketName"] == bucketName)["bucketId"];

            Console.WriteLine($" Done.  Bucket ID: {bucketID}");


            // Get Upload URL
            Console.Write("Getting Upload URL... ");

            request = new RestRequest("/b2api/v1/b2_get_upload_url", Method.POST);
            request.RequestFormat = DataFormat.Json;
            request.AddHeader("Authorization", authorizationToken);
            request.AddBody(new { bucketId = bucketID });

            response = client.Execute(request);
            content = response.Content;

            if (response.StatusCode != HttpStatusCode.OK)
            {
                Console.WriteLine(content);
                return 1;
            }

            var uploadURL = (string)JObject.Parse(content)["uploadUrl"];
            var uploadAuthToken = (string)JObject.Parse(content)["authorizationToken"];
            Console.WriteLine("Done.");


            // Upload File
            var uploadResult = UploadFile(fileToUpload, uploadURL, uploadAuthToken);

            if (!uploadResult)
                return 1;

            Console.WriteLine(Environment.NewLine + "All Done!");

            return 0;
        }

        /// <summary>
        /// Uploads a file.  Configured with B2's required headers
        /// </summary>
        /// <param name="fileToUpload">Path of file to upload</param>
        /// <param name="uploadURL">B2 upload URL</param>
        /// <param name="uploadAuthToken">B2 upload token</param>
        /// <returns></returns>
        public static bool UploadFile(string fileToUpload, string uploadURL, string uploadAuthToken)
        {
            Console.WriteLine("Uploading File: ");

            var hash = ComputeHash(fileToUpload, new SHA1CryptoServiceProvider());
            var encodedFileName = Uri.EscapeDataString(Path.GetFileName(fileToUpload));

            var uploadFileInfo = new FileInfo(fileToUpload);

            var uploadRequest = (HttpWebRequest)HttpWebRequest.Create(uploadURL);

            uploadRequest.Headers["Authorization"] = uploadAuthToken;

            uploadRequest.ReadWriteTimeout = (int)TimeSpan.FromMinutes(10).TotalMilliseconds;
            uploadRequest.Timeout = (int)TimeSpan.FromMinutes(10).TotalMilliseconds;
            uploadRequest.AllowWriteStreamBuffering = false;
            uploadRequest.SendChunked = false;
            uploadRequest.KeepAlive = true;
            uploadRequest.Method = "POST";
            uploadRequest.ContentType = "b2/x-auto";
            uploadRequest.ContentLength = uploadFileInfo.Length;

            uploadRequest.Headers["X-Bz-File-Name"] = encodedFileName;
            uploadRequest.Headers["X-Bz-Content-Sha1"] = hash;

            uploadRequest.Headers["X-Bz-Info-UploaderVer"] = "1";

            var segments = Convert.ToInt32(uploadFileInfo.Length / (uploadBuffer));

            try
            {
                // Start the upload
                using (Stream st = uploadRequest.GetRequestStream())
                {
                    using (FileStream fs = new FileStream(fileToUpload, FileMode.Open, FileAccess.Read))
                    {
                        byte[] buffer = new byte[uploadBuffer];
                        int read;
                        double count = 0.0;

                        while ((read = fs.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            st.Write(buffer, 0, read);

                            // report progress back
                            var percentComplete = (segments > 0) ? (count / (double)segments) : 1;
                            Console.Write("\r{0:P2}  ", percentComplete);

                            count++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to Upload file: {ex.Message}");
                return false;
            }

            Console.Write("Finalizing Upload... ");
            try
            {
                using (HttpWebResponse uploadResponse = (HttpWebResponse)uploadRequest.GetResponse())
                {
                    // Verify the upload
                    using (var buffer = new BufferedStream(uploadResponse.GetResponseStream()))
                    {
                        using (var reader = new StreamReader(buffer))
                        {
                            var pageContents = reader.ReadToEnd();
                            var jUpload = JObject.Parse(pageContents);
                            var uploadFileId = (string)jUpload["fileId"];

                            Console.WriteLine($"Done.  File ID: {uploadFileId}");
                        }
                    }
                }
            }
            catch (WebException wex)
            {
                if (wex != null)
                {
                    var response = new StreamReader(wex?.Response?.GetResponseStream()).ReadToEnd();
                    Console.WriteLine(response);
                }

                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while processing upload: {ex.Message}");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Computes a file hash without loading the entire file into memory.
        /// </summary>
        /// <param name="filePath">Path of the file to hash.</param>
        /// <param name="hashAlgorithm">Algorithm to hash in.  Example: 'new SHA1CryptoServiceProvider()'</param>
        /// <returns></returns>
        public static string ComputeHash(string filePath, HashAlgorithm hashAlgorithm)
        {
            try
            {
                using (var stream = (Stream)File.Open(filePath, FileMode.Open))
                {
                    int _bufferSize = 4096; // this makes it impossible to change the buffer size while computing

                    byte[] readAheadBuffer, buffer;
                    int readAheadBytesRead, bytesRead;
                    long size, totalBytesRead = 0;

                    size = stream.Length;
                    readAheadBuffer = new byte[_bufferSize];
                    readAheadBytesRead = stream.Read(readAheadBuffer, 0, readAheadBuffer.Length);

                    totalBytesRead += readAheadBytesRead;

                    do
                    {
                        bytesRead = readAheadBytesRead;
                        buffer = readAheadBuffer;

                        readAheadBuffer = new byte[_bufferSize];
                        readAheadBytesRead = stream.Read(readAheadBuffer, 0, readAheadBuffer.Length);

                        totalBytesRead += readAheadBytesRead;

                        if (readAheadBytesRead == 0)
                            hashAlgorithm.TransformFinalBlock(buffer, 0, bytesRead);
                        else
                            hashAlgorithm.TransformBlock(buffer, 0, bytesRead, buffer, 0);
                    } while (readAheadBytesRead != 0);
                }

                string hex = "";
                foreach (byte b in hashAlgorithm.Hash)
                    hex += b.ToString("x2");

                return hex.ToLower();
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
