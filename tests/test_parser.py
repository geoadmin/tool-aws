import sys
import mock
import unittest
from tool_aws.s3.rm import createParser, parseArguments


class TestS3Utils(unittest.TestCase):

    def test_parser_missing_prefix(self):
        parser = createParser()
        testArgvs = ['s3rm', '--bucket-name', 'myDummyBucket']
        with mock.patch.object(sys, 'argv', testArgvs):
            with self.assertRaises(BaseException):
                parseArguments(parser, sys.argv)

    def test_parser_missing_bucket_name(self):
        parser = createParser()
        testArgvs = ['s3rm', '--prefix', '/1.0.0/ch.dummy/default/2056/*']
        with mock.patch.object(sys, 'argv', testArgvs):
            with self.assertRaises(BaseException):
                parseArguments(parser, sys.argv)

    def test_parser_only_required(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*']
        with mock.patch.object(sys, 'argv', testArgvs):
            opts, srids = parseArguments(parser, sys.argv)
            self.assertEqual(opts.prefix, '/1.0.0/ch.dummy/default/2056/')
            self.assertEqual(opts.bucketName, 'myDummyBucket')
            self.assertEqual(opts.profileName, 'default')
            self.assertEqual(len(srids), 0)

    def test_parser_with_thread_nb(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--threads-number', '2']
        with mock.patch.object(sys, 'argv', testArgvs):
            opts, srids = parseArguments(parser, sys.argv)
            self.assertEqual(opts.prefix, '/1.0.0/ch.dummy/default/2056/')
            self.assertEqual(opts.bucketName, 'myDummyBucket')
            self.assertEqual(opts.profileName, 'default')
            self.assertEqual(len(srids), 0)
            self.assertEqual(opts.nbThreads, 2)

    def test_parser_with_force_true(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--force']
        with mock.patch.object(sys, 'argv', testArgvs):
            opts, srids = parseArguments(parser, sys.argv)
            self.assertEqual(opts.prefix, '/1.0.0/ch.dummy/default/2056/')
            self.assertEqual(opts.bucketName, 'myDummyBucket')
            self.assertEqual(opts.profileName, 'default')
            self.assertEqual(len(srids), 0)
            self.assertEqual(opts.force, True)

    def test_parser_with_bad_bbox(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--bbox', '100,200,150,250']
        with mock.patch.object(sys, 'argv', testArgvs):
            with self.assertRaises(BaseException):
                parseArguments(parser, sys.argv)

    def test_parser_with_bbox_only(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--bbox', '1200000,2200000,1500000,2500000']
        with mock.patch.object(sys, 'argv', testArgvs):
            with self.assertRaises(BaseException):
                parseArguments(parser, sys.argv)

    def test_parser_with_bbox_and_img_format(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--bbox', '1200000,2200000,1500000,2500000',
            '--image-format', 'png']
        with mock.patch.object(sys, 'argv', testArgvs):
            opts, srids = parseArguments(parser, sys.argv)
            self.assertEqual(opts.prefix, '/1.0.0/ch.dummy/default/2056/')
            self.assertEqual(opts.bucketName, 'myDummyBucket')
            self.assertEqual(opts.profileName, 'default')
            self.assertEqual(len(srids), 4)
            self.assertEqual(','.join([str(int(b)) for b in opts.bbox]),
                             '1200000,2200000,1500000,2500000')

    def test_parser_with_bbox_and_bad_img_format(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--bbox', '1200000,2200000,1500000,2500000',
            '--image-format', 'gif']
        with mock.patch.object(sys, 'argv', testArgvs):
            with self.assertRaises(BaseException):
                parseArguments(parser, sys.argv)

    def test_parser_with_bbox_and_threads(self):
        parser = createParser()
        testArgvs = [
            's3rm',
            '--bucket-name', 'myDummyBucket',
            '--prefix', '/1.0.0/ch.dummy/default/2056/*',
            '--bbox', '1200000,2200000,1500000,2500000',
            '--image-format', 'jpeg',
            '-n', '3']
        with mock.patch.object(sys, 'argv', testArgvs):
            opts, srids = parseArguments(parser, sys.argv)
            self.assertEqual(opts.prefix, '/1.0.0/ch.dummy/default/2056/')
            self.assertEqual(opts.bucketName, 'myDummyBucket')
            self.assertEqual(opts.profileName, 'default')
            self.assertEqual(opts.nbThreads, 3)
            self.assertEqual(len(srids), 4)
            self.assertEqual(','.join([str(int(b)) for b in opts.bbox]),
                             '1200000,2200000,1500000,2500000')
