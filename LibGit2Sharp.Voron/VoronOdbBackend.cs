using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using Voron;
using Voron.Impl;

namespace LibGit2Sharp.Voron
{
    public class VoronOdbBackend : OdbBackend
    {
        private const string MetaPrefix = "meta/";
        private const string DataPrefix = "data/";

        private readonly StorageEnvironment _env;

        public VoronOdbBackend(string voronDataPath)
        {
            if (voronDataPath == null)
            {
                throw new ArgumentNullException("voronDataPath");
            }

            var memoryMapPager = new MemoryMapPager(voronDataPath);
            _env = new StorageEnvironment(memoryMapPager);
        }

        protected override void Dispose()
        {
            _env.Dispose();
            base.Dispose();
        }

        public override int Read(ObjectId id, out Stream data, out ObjectType objectType)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                using (ReadResult readResult = _env.Root.Read(tx, ToMetaId(id)))
                {
                    Debug.Assert(readResult != null);

                    var bf = new BinaryFormatter();
                    var meta = (ObjectDescriptor)bf.Deserialize(readResult.Stream);

                    objectType = meta.ObjectType;
                    data = Allocate(meta.Length);
                }

                using (ReadResult readResult = _env.Root.Read(tx, ToDataId(id)))
                {
                    readResult.Stream.CopyTo(data);
                }
            }

            return (int)ReturnCode.GIT_OK;
        }

        public override int ReadPrefix(byte[] shortOid, int prefixLen, out byte[] oid, out Stream data, out ObjectType objectType)
        {
            throw new NotImplementedException();
        }

        public override int ReadHeader(ObjectId id, out int length, out ObjectType objectType)
        {
            throw new NotImplementedException();
        }

        public override int Write(ObjectId id, Stream dataStream, long length, ObjectType objectType)
        {
            var objDescriptor = new ObjectDescriptor { Length = length, ObjectType = objectType };

            var bf = new BinaryFormatter();

            using (var ms = new MemoryStream())
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                bf.Serialize(ms, objDescriptor);
                ms.Seek(0, SeekOrigin.Begin);

                _env.Root.Add(tx, ToMetaId(id), ms);
                _env.Root.Add(tx, ToDataId(id), dataStream);
                tx.Commit();
            }

            return (int)ReturnCode.GIT_OK;
        }

        private static string ToDataId(ObjectId id)
        {
            return DataPrefix + id.Sha;
        }

        private static string ToMetaId(ObjectId id)
        {
            return MetaPrefix + id.Sha;
        }

        public override int ReadStream(ObjectId id, out OdbBackendStream stream)
        {
            throw new NotImplementedException();
        }

        public override int WriteStream(long length, ObjectType objectType, out OdbBackendStream stream)
        {
            stream = new VoronOdbBackendWriteOnlyStream(this, objectType, length);

            return (int)ReturnCode.GIT_OK;
        }

        public override bool Exists(ObjectId id)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            using (var stream = _env.Root.Read(tx, ToMetaId(id)))
            {
                return stream != null;
            }
        }

        public override int ForEach(ForEachCallback callback)
        {
            throw new NotImplementedException();
        }

        protected override OdbBackendOperations SupportedOperations
        {
            get
            {
                return OdbBackendOperations.Read |
                       OdbBackendOperations.Write |
                       OdbBackendOperations.WriteStream |
                       OdbBackendOperations.Exists;
            }
        }

        [Serializable]
        private class ObjectDescriptor
        {
            public ObjectType ObjectType { get; set; }
            public long Length { get; set; }
        }

        private class VoronOdbBackendWriteOnlyStream : OdbBackendStream
        {
            private readonly List<byte[]> _chunks = new List<byte[]>();

            private readonly ObjectType _type;
            private readonly long _length;

            public VoronOdbBackendWriteOnlyStream(VoronOdbBackend backend, ObjectType objectType, long length)
                : base(backend)
            {
                _type = objectType;
                _length = length;
            }

            public override bool CanRead
            {
                get
                {
                    return false;
                }
            }

            public override bool CanWrite
            {
                get
                {
                    return true;
                }
            }

            public override int Write(Stream dataStream, long length)
            {
                var buffer = new byte[length];

                int bytesRead = dataStream.Read(buffer, 0, Convert.ToInt32(length));

                if (bytesRead != (int)length)
                {
                    throw new InvalidOperationException(
                        string.Format("Too short buffer. {0} bytes were expected. {1} have been successfully read.",
                            length, bytesRead));
                }

                _chunks.Add(buffer);

                return (int)ReturnCode.GIT_OK;
            }

            public override int FinalizeWrite(ObjectId oid)
            {
                //TODO: Drop the check of the size when libgit2 #1837 is merged
                long totalLength = _chunks.Sum(chunk => chunk.Length);

                if (totalLength != _length)
                {
                    throw new InvalidOperationException(
                        string.Format("Invalid object length. {0} was expected. The "
                                      + "total size of the received chunks amounts to {1}.",
                                      _length, totalLength));
                }

                using (Stream stream = new FakeStream(_chunks, _length))
                {
                    Backend.Write(oid, stream, _length, _type);
                }

                return (int)ReturnCode.GIT_OK;
            }

            public override int Read(Stream dataStream, long length)
            {
                throw new NotImplementedException();
            }

            private class FakeStream : Stream
            {
                private readonly IList<byte[]> _chunks;
                private readonly long _length;
                public int currentChunk = 0;
                public int currentPos = 0;

                public FakeStream(IList<byte[]> chunks, long length)
                {
                    _chunks = chunks;
                    _length = length;
                }

                public override void Flush()
                {
                    throw new NotImplementedException();
                }

                public override long Seek(long offset, SeekOrigin origin)
                {
                    throw new NotImplementedException();
                }

                public override void SetLength(long value)
                {
                    throw new NotImplementedException();
                }

                public override int Read(byte[] buffer, int offset, int count)
                {
                    var totalCopied = 0;

                    while (totalCopied < count)
                    {
                        if (currentChunk > _chunks.Count - 1)
                        {
                            return totalCopied;
                        }

                        var toBeCopied = Math.Min(_chunks[currentChunk].Length - currentPos, count - totalCopied);

                        Buffer.BlockCopy(_chunks[currentChunk], currentPos, buffer, offset + totalCopied, toBeCopied);
                        currentPos += toBeCopied;
                        totalCopied += toBeCopied;

                        Debug.Assert(currentPos <= _chunks[currentChunk].Length);

                        if (currentPos == _chunks[currentChunk].Length)
                        {
                            currentPos = 0;
                            currentChunk++;
                        }
                    }

                    return totalCopied;
                }

                public override void Write(byte[] buffer, int offset, int count)
                {
                    throw new NotImplementedException();
                }

                public override bool CanRead
                {
                    get { return true; }
                }

                public override bool CanSeek
                {
                    get { throw new NotImplementedException(); }
                }

                public override bool CanWrite
                {
                    get { throw new NotImplementedException(); }
                }

                public override long Length
                {
                    get { return _length; }
                }

                public override long Position
                {
                    get { throw new NotImplementedException(); }
                    set { throw new NotImplementedException(); }
                }
            }
        }
    }
}
