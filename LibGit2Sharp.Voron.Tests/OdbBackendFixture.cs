using System;
using System.IO;
using System.Linq;
using Xunit;
using Xunit.Extensions;

namespace LibGit2Sharp.Voron.Tests
{
    public class OdbBackendFixture : IDisposable
    {
        private static readonly string TempPath = Path.Combine(Path.GetTempPath(), "Voron.OdbBackend");

        public OdbBackendFixture()
        {
            Directory.CreateDirectory(TempPath);
        }

        public void Dispose()
        {
            Helper.DeleteDirectory(TempPath);
        }

        private static Repository InitNewRepository(string baseDir)
        {
            string tempPath = Path.Combine(baseDir, Guid.NewGuid().ToString());
            string path = Repository.Init(tempPath);

            var repository = new Repository(path);

            return repository;
        }

        private Repository Build(bool isVoronBased)
        {
            var repository = InitNewRepository(TempPath);

            if (!isVoronBased)
            {
                return repository;
            }

            var dir = new DirectoryInfo(repository.Info.WorkingDirectory);

            string voronDataFilename = string.Format("{0}-voron.data", dir.Name.Substring(0, 7));
            string voronDataPath = Path.Combine(TempPath, voronDataFilename);

            repository.ObjectDatabase.AddBackend(new VoronOdbBackend(voronDataPath), priority: 5);

            return repository;
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CanStageAFileAndLookupTheGeneratedBlob(bool isVoronBased)
        {
            using (var repo = Build(isVoronBased))
            {
                const string path = "test.txt";
                const string content = "test\n";

                var objectId = new ObjectId("9daeafb9864cf43055ae93beb0afd6c7d144bfa4");

                //TODO: This triggers two calls to Exists() Why?
                Assert.False(repo.ObjectDatabase.Contains(objectId));
                Assert.Null(repo.Lookup<Blob>(objectId));

                Helper.Touch(repo.Info.WorkingDirectory, path, content);
                repo.Index.Stage(path);

                var ie = repo.Index[path];
                Assert.NotNull(ie);
                Assert.Equal(objectId, ie.Id);

                Assert.True(repo.ObjectDatabase.Contains(ie.Id));

                //TODO: Maybe lookup of a blob should only trigger read_header()
                var blob = repo.Lookup<Blob>(objectId);
                Assert.Equal(content.Length, blob.Size);
                Assert.Equal(content, blob.ContentAsText());
            }
        }

        private static void AddCommitToRepo(Repository repo)
        {
            const string path = "test.txt";
            const string content = "test\n";

            Helper.Touch(repo.Info.WorkingDirectory, path, content);
            repo.Index.Stage(path);

            var author = new Signature("nulltoken", "emeric.fermas@gmail.com", DateTimeOffset.Parse("Wed, Dec 14 2011 08:29:03 +0100"));
            repo.Commit("Initial commit", author, author);
        }

        private static void AssertGeneratedShas(Repository repo)
        {
            Commit commit = repo.Commits.Single();
            Assert.Equal("1fe3126578fc4eca68c193e4a3a0a14a0704624d", commit.Sha);
            Tree tree = commit.Tree;
            Assert.Equal("2b297e643c551e76cfa1f93810c50811382f9117", tree.Sha);

            GitObject blob = tree.Single().Target;
            Assert.IsAssignableFrom<Blob>(blob);
            Assert.Equal("9daeafb9864cf43055ae93beb0afd6c7d144bfa4", blob.Sha);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CanGeneratePredictableObjectShas(bool isVoronBased)
        {
            using (var repo = Build(isVoronBased))
            {
                AddCommitToRepo(repo);

                AssertGeneratedShas(repo);
            }
        }
    }
}
