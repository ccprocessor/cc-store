"""
Tests for utility functions.
"""

import unittest

import mmh3

from cc_store.utils.hash_utils import (
    compute_content_hash,
    compute_simhash,
    hamming_distance,
    is_similar
)


class TestHashUtils(unittest.TestCase):
    """Tests for hash utility functions."""
    
    def test_compute_content_hash(self):
        """Test compute_content_hash function."""
        # Empty content
        self.assertEqual(compute_content_hash(""), "empty")
        
        # Same content should have same hash
        content1 = "Hello, world!"
        content2 = "Hello, world!"
        hash1 = compute_content_hash(content1)
        hash2 = compute_content_hash(content2)
        self.assertEqual(hash1, hash2)
        
        # Different content should have different hash
        content3 = "Hello, World!"  # Capital 'W'
        hash3 = compute_content_hash(content3)
        self.assertNotEqual(hash1, hash3)
        
        # Test different methods
        sha256_hash = compute_content_hash(content1, method="sha256")
        murmur_hash = compute_content_hash(content1, method="murmur3")
        self.assertNotEqual(sha256_hash, murmur_hash)
        
        # Test seed affects murmur hash
        hash_seed1 = compute_content_hash(content1, method="murmur3", seed=1)
        hash_seed2 = compute_content_hash(content1, method="murmur3", seed=2)
        self.assertNotEqual(hash_seed1, hash_seed2)
    
    def test_compute_simhash(self):
        """Test compute_simhash function."""
        # Empty content
        self.assertEqual(compute_simhash(""), 0)
        
        # Similar content should have similar SimHash values
        content1 = "<html><body><h1>Hello World</h1><p>This is example 1</p></body></html>"
        content2 = "<html><body><h1>Hello World</h1><p>This is example 2</p></body></html>"
        
        simhash1 = compute_simhash(content1)
        simhash2 = compute_simhash(content2)
        
        # The Hamming distance should be small for similar content
        distance = hamming_distance(simhash1, simhash2)
        self.assertLess(distance, 10)
        
        # Very different content should have different SimHash values
        content3 = "<html><body><h1>Completely Different</h1><p>Nothing similar here</p></body></html>"
        simhash3 = compute_simhash(content3)
        
        # The Hamming distance should be larger for different content
        distance = hamming_distance(simhash1, simhash3)
        self.assertGreater(distance, 10)
    
    def test_hamming_distance(self):
        """Test hamming_distance function."""
        # Same values have distance 0
        self.assertEqual(hamming_distance(0, 0), 0)
        self.assertEqual(hamming_distance(42, 42), 0)
        
        # Simple cases
        self.assertEqual(hamming_distance(0, 1), 1)  # Only the lowest bit differs
        self.assertEqual(hamming_distance(0, 3), 2)  # Two lowest bits differ
        
        # Calculate manually for a specific case
        a = 0b1010
        b = 0b1001
        # Bits that differ are at positions 1 and 3, so distance should be 2
        self.assertEqual(hamming_distance(a, b), 2)
    
    def test_is_similar(self):
        """Test is_similar function."""
        # Same values are similar
        self.assertTrue(is_similar(0, 0))
        self.assertTrue(is_similar(42, 42))
        
        # Values with small Hamming distance are similar
        self.assertTrue(is_similar(0b1010, 0b1011, threshold=1))  # Distance is 1
        self.assertTrue(is_similar(0b1010, 0b1001, threshold=2))  # Distance is 2
        
        # Values with large Hamming distance are not similar
        self.assertFalse(is_similar(0b1010, 0b0101, threshold=2))  # Distance is 4
        
        # Test with different thresholds
        a = 0b10101010
        b = 0b10101001  # Differs in 2 bits
        
        self.assertTrue(is_similar(a, b, threshold=2))
        self.assertFalse(is_similar(a, b, threshold=1))


if __name__ == "__main__":
    unittest.main() 