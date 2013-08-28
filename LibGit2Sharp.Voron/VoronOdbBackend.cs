using System;
using System.Diagnostics;
using System.IO;
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
                using (var stream = _env.Root.Read(tx, ToMetaId(id)))
                {
                    Debug.Assert(stream != null);

                    var bf = new BinaryFormatter();
                    var meta = (ObjectDescriptor)bf.Deserialize(stream);

                    objectType = meta.ObjectType;
                    data = Allocate(meta.Length);
                }

                using (var stream = _env.Root.Read(tx, ToDataId(id)))
                {
                    stream.CopyTo(data);
                }
            }

            return 0;
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

            return 0;
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
            throw new NotImplementedException();
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
                       OdbBackendOperations.Exists;
            }
        }

        [Serializable]
        private class ObjectDescriptor
        {
            public ObjectType ObjectType { get; set; }
            public long Length { get; set; }
        }
    }
}
