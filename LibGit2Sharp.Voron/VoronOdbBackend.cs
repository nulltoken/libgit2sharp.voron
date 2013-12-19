using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using Voron;

namespace LibGit2Sharp.Voron
{
    public class VoronOdbBackend : OdbBackend
    {
        private const string Index = "idx";
        private const string Objects = "obj";

        private readonly StorageEnvironment _env;

        public VoronOdbBackend(string voronDataPath)
        {
            if (voronDataPath == null)
            {
                throw new ArgumentNullException("voronDataPath");
            }

            _env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(voronDataPath));

            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                _env.CreateTree(tx, Index);
                _env.CreateTree(tx, Objects);
                tx.Commit();
            }
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
                ReadResult readResult1 = tx.GetTree(Index).Read(tx, id.Sha);
                Debug.Assert(readResult1 != null);

                var bf = new BinaryFormatter();
                var meta = (ObjectDescriptor) bf.Deserialize(readResult1.Reader.AsStream());

                objectType = meta.ObjectType;
                data = Allocate(meta.Length);

                ReadResult readResult2 = tx.GetTree(Objects).Read(tx, id.Sha);
                readResult2.Reader.CopyTo(data);
            }

            return (int)ReturnCode.GIT_OK;
        }

        public override int ReadPrefix(byte[] shortOid, int prefixLen, out byte[] oid, out Stream data, out ObjectType objectType)
        {
            oid = null;
            data = null;
            objectType = default(ObjectType);

            ObjectId matchingKey = null;
            bool moreThanOneMatchingKeyHasBeenFound = false;

            int ret = ForEach(objectId =>
            {
                if (!objectId.StartsWith(shortOid, prefixLen))
                {
                    return (int)ReturnCode.GIT_OK;
                }

                if (matchingKey != null)
                {
                    moreThanOneMatchingKeyHasBeenFound = true;
                    return (int)ReturnCode.GIT_EAMBIGUOUS;
                }

                matchingKey = objectId;

                return (int)ReturnCode.GIT_OK;
            });

            if (ret != (int)ReturnCode.GIT_OK
                && ret != (int)ReturnCode.GIT_EUSER)
            {
                return ret;
            }

            if (moreThanOneMatchingKeyHasBeenFound)
            {
                return (int) ReturnCode.GIT_EAMBIGUOUS;
            }

            ret = Read(matchingKey, out data, out objectType);

            if (ret != (int)ReturnCode.GIT_OK)
            {
                return ret;
            }

            oid = matchingKey.RawId;

            return (int)ReturnCode.GIT_OK;
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

                tx.GetTree(Index).Add(tx, id.Sha, ms);
                tx.GetTree(Objects).Add(tx, id.Sha, dataStream);
                tx.Commit();
            }

            return (int)ReturnCode.GIT_OK;
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
            {
                var readResult = tx.GetTree(Index).Read(tx, id.Sha);
                return readResult != null;
            }
        }

        public override int ForEach(ForEachCallback callback)
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            using (var it = tx.GetTree(Index).Iterate(tx))
            {
                if (it.Seek(Slice.BeforeAllKeys) == false)
                {
                    return (int)ReturnCode.GIT_OK;
                }

                do
                {
                    string sha = it.CurrentKey.ToString();

                    int ret = callback(new ObjectId(sha));

                    if (ret != (int)ReturnCode.GIT_OK)
                    {
                        return (int)ReturnCode.GIT_EUSER;
                    }
                } while (it.MoveNext());
            }

            return (int)ReturnCode.GIT_OK;
        }

        protected override OdbBackendOperations SupportedOperations
        {
            get
            {
                return OdbBackendOperations.Read |
                       OdbBackendOperations.Write |
                       OdbBackendOperations.ReadPrefix |
                       OdbBackendOperations.WriteStream |
                       OdbBackendOperations.Exists |
                       OdbBackendOperations.ForEach;
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

                int offset = 0, bytesRead;
                int toRead = Convert.ToInt32(length);

                do
                {
                    toRead -= offset;
                    bytesRead = dataStream.Read(buffer, offset, toRead);
                    offset += bytesRead;
                } while (bytesRead != 0);

                if (offset != (int)length)
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
                private int currentChunk = 0;
                private int currentPos = 0;

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
