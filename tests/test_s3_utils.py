import mock
import unittest
import collections
from tool_aws.s3.utils import S3Keys, getMaxChunkSize


class DummyS3Bucket(dict):
    pass

dummyS3Bucket = DummyS3Bucket()
dummyS3Bucket.name = 'myDummyBucketName'


NB_KEYS = 101


class TestS3Utils(unittest.TestCase):

    def setUp(self):
        def getOrderedKeys(a, b, c):
            return [{'Key': str(i)} for i in range(NB_KEYS)]
        self.getKeysFromS3 = getOrderedKeys
        self.getKeysFromS3_patch = mock.patch(
            'tool_aws.s3.utils.getKeysFromS3', self.getKeysFromS3)
        self.getKeysFromS3_patch.start()

    def tearDown(self):
        self.getKeysFromS3_patch.stop()

    def test_s3_keys_with_chunksize(self):
        prefix = 'foo/'
        chunkSize = 20
        dummyKeys = S3Keys(dummyS3Bucket, prefix, chunkSize)

        self.assertIsInstance(dummyKeys, collections.Iterable)
        self.assertEqual(len(dummyKeys), NB_KEYS)
        self.assertEqual(dummyKeys.prefix, prefix)
        self.assertEqual(dummyKeys.chunkSize, chunkSize)
        self.assertIn('20', str(dummyKeys))
        self.assertIn('101', str(dummyKeys))
        self.assertIn('myDummyBucketName', str(dummyKeys))

        firstChunk = next(dummyKeys.chunkedKeys)
        self.assertEqual(len(firstChunk), chunkSize)
        self.assertIn('Key', firstChunk[0])
        secondChunk = next(dummyKeys.chunkedKeys)
        self.assertEqual(len(secondChunk), chunkSize)
        self.assertIn('Key', secondChunk[0])
        next(dummyKeys.chunkedKeys)
        next(dummyKeys.chunkedKeys)
        next(dummyKeys.chunkedKeys)
        sixthChunk = next(dummyKeys.chunkedKeys)
        self.assertEqual(len(sixthChunk), 1)

    def test_s3_keys_no_chunksize(self):
        prefix = 'foo/'
        dummyKeys = S3Keys(dummyS3Bucket, prefix)

        self.assertIsInstance(dummyKeys, collections.Iterable)
        self.assertEqual(len(dummyKeys), NB_KEYS)
        self.assertEqual(dummyKeys.prefix, prefix)
        self.assertEqual(dummyKeys.chunkSize, 1)

        firstChunk = next(dummyKeys.chunkedKeys)
        self.assertEqual(len(firstChunk), 1)
        self.assertIn('Key', firstChunk[0])
        secondChunk = next(dummyKeys.chunkedKeys)
        self.assertEqual(len(secondChunk), 1)
        self.assertIn('Key', secondChunk[0])

    def test_s3_keys_chunk(self):
        prefix = 'foo/'
        dummyKeys = S3Keys(dummyS3Bucket, prefix)

        self.assertIsInstance(dummyKeys, collections.Iterable)
        self.assertEqual(len(dummyKeys), NB_KEYS)
        self.assertEqual(dummyKeys.prefix, prefix)
        self.assertEqual(dummyKeys.chunkSize, 1)

        chunkSize = 20
        dummyKeys.chunk(chunkSize)
        self.assertIsInstance(dummyKeys, collections.Iterable)
        self.assertEqual(len(dummyKeys), NB_KEYS)
        self.assertEqual(dummyKeys.prefix, prefix)
        self.assertEqual(dummyKeys.chunkSize, chunkSize)

        chunkSize = 1
        dummyKeys.chunk(chunkSize)
        self.assertIsInstance(dummyKeys, collections.Iterable)
        self.assertEqual(len(dummyKeys), NB_KEYS)
        self.assertEqual(dummyKeys.prefix, prefix)
        self.assertEqual(dummyKeys.chunkSize, chunkSize)

        keys = [k for k in dummyKeys]
        self.assertEqual(len(keys), NB_KEYS)

    def test_get_max_chunk_size(self):
        nbProc = 8
        nbKeys = 799
        chunkSize = getMaxChunkSize(nbProc, nbKeys)
        self.assertEqual(chunkSize, 99)

        nbKeys = 800
        chunkSize = getMaxChunkSize(nbProc, nbKeys)
        self.assertEqual(chunkSize, 100)

        nbKeys = 10000
        chunkSize = getMaxChunkSize(nbProc, nbKeys)
        self.assertEqual(chunkSize, 1000)

        nbKeys = 1
        chunkSize = getMaxChunkSize(nbProc, nbKeys)
        self.assertEqual(chunkSize, 1)

        nbKeys = 0
        chunkSize = getMaxChunkSize(nbProc, nbKeys)
        self.assertEqual(chunkSize, 0)
