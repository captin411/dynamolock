#!/usr/bin/env python
import datetime
import unittest
from dynamolock.lock import DynamoDBLock
from dynamolock.policy import DynamoDBLockPolicy


class DynamoDBLockTest(unittest.TestCase):

    def test_lock_init(self):
        params = {
            'name':      'my.lock.name',
            'owner':     'host.company.org.123e4567-e89b-12d3-a456-426655440000',
            'timestamp': 1406929231,
            'is_locked': True,
            'duration':  5000000,
            'version':  '123e4567-e89b-12d3-a456-426655440000',
            'payload':   None,
        }
        lock = DynamoDBLock(**params)
        self.assertIsNotNone(lock)

    def test_lock_copy(self):
        params = {
            'name':      'my.lock.name',
            'owner':     'host.company.org.123e4567-e89b-12d3-a456-426655440000',
            'timestamp': 1406929231,
            'is_locked': True,
            'duration':  5000000,
            'version':  '123e4567-e89b-12d3-a456-426655440000',
            'payload':   None,
        }
        old_lock = DynamoDBLock(**params)
        new_lock = old_lock._replace(owner='another.company.org.123e4567-e89b-12d3-a456-426655440000')

        self.assertIsNotNone(old_lock)
        self.assertIsNotNone(new_lock)
        self.assertNotEqual(old_lock, new_lock)


class DynamoDBLockPolicyTest(unittest.TestCase):

    def test_lock_init_default(self):
        policy = DynamoDBLockPolicy()
        self.assertEqual(policy.acquire_timeout, 10 * 1000)
        self.assertEqual(policy.retry_period, 10)
        self.assertEqual(policy.lock_duration, 60 * 1000)
        self.assertEqual(policy.delete_lock, True)

    def test_lock_init_acquire(self):
        seconds = 60
        policy = DynamoDBLockPolicy(
            acquire_timeout=datetime.timedelta(seconds=seconds)
        )
        self.assertEqual(policy.acquire_timeout, seconds * 1000)

        with self.assertRaisesRegexp(
                AttributeError,
                ".*object has no attribute 'total_seconds'.*"):
            policy = DynamoDBLockPolicy(acquire_timeout=seconds)

    def test_lock_init_retry_period(self):
        seconds = 60
        policy = DynamoDBLockPolicy(
            retry_period=datetime.timedelta(seconds=seconds)
        )
        self.assertEqual(policy.retry_period, seconds)

        with self.assertRaisesRegexp(
                AttributeError,
                ".*object has no attribute 'total_seconds'.*"):
            policy = DynamoDBLockPolicy(retry_period=seconds)

    def test_lock_init_lock_duration(self):
        seconds = 60
        policy = DynamoDBLockPolicy(
            lock_duration=datetime.timedelta(seconds=seconds)
        )
        self.assertEqual(policy.lock_duration, seconds * 1000)

        with self.assertRaisesRegexp(
                AttributeError,
                ".*object has no attribute 'total_seconds'.*"):
            policy = DynamoDBLockPolicy(lock_duration=seconds)

    def test_lock_init_delete_lock(self):
        policy = DynamoDBLockPolicy(
            delete_lock=False
        )
        self.assertEqual(policy.delete_lock, False)

# ---------------------------------------------------------------------------#
# main
# ---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
