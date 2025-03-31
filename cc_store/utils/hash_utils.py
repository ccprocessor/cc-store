"""
Hash utility functions.
"""

import hashlib
import mmh3
from typing import Optional


def compute_content_hash(content: str, method: str = "murmur3", seed: int = 42) -> str:
    """
    Compute a hash for the given content.
    
    Args:
        content: Content to hash
        method: Hash method ('sha256', 'murmur3')
        seed: Seed for MurmurHash
        
    Returns:
        String hash value
    """
    if not content:
        return "empty"
        
    if method == "sha256":
        return hashlib.sha256(content.encode("utf-8")).hexdigest()
    elif method == "murmur3":
        # MurmurHash is faster than SHA-256 and still has low collision probability
        hash_value = mmh3.hash(content, seed=seed)
        return f"{hash_value:x}"  # Convert to hex string
    else:
        raise ValueError(f"Unsupported hash method: {method}")


def compute_simhash(content: str, shingle_size: int = 4, seed: int = 42) -> int:
    """
    Compute a SimHash for the given content.
    
    SimHash is a technique for fuzzy hashing, allowing similarity-based deduplication.
    
    Args:
        content: Content to hash
        shingle_size: Size of text shingles
        seed: Seed for MurmurHash
        
    Returns:
        Integer SimHash value
    """
    if not content:
        return 0
    
    # Tokenize content into shingles
    shingles = []
    for i in range(len(content) - shingle_size + 1):
        shingle = content[i:i + shingle_size]
        shingles.append(shingle)
    
    # Use 64-bit hashes
    bit_vector = [0] * 64
    
    # Hash each shingle and update the bit vector
    for shingle in shingles:
        # Use MurmurHash for speed
        h = mmh3.hash64(shingle, seed=seed)[0]
        
        # Update bit vector
        for i in range(64):
            bit_mask = 1 << i
            if h & bit_mask:
                bit_vector[i] += 1
            else:
                bit_vector[i] -= 1
    
    # Convert bit vector to hash
    simhash = 0
    for i in range(64):
        if bit_vector[i] > 0:
            simhash |= (1 << i)
    
    return simhash


def hamming_distance(hash1: int, hash2: int) -> int:
    """
    Calculate the Hamming distance between two hashes.
    
    The Hamming distance is the number of bit positions in which the two hashes differ.
    
    Args:
        hash1: First hash
        hash2: Second hash
        
    Returns:
        Hamming distance
    """
    xor = hash1 ^ hash2
    distance = 0
    
    # Count the number of set bits in the XOR result
    while xor:
        distance += 1
        xor &= xor - 1  # Clear the least significant bit
    
    return distance


def is_similar(hash1: int, hash2: int, threshold: int = 3) -> bool:
    """
    Check if two hashes are similar based on Hamming distance.
    
    Args:
        hash1: First hash
        hash2: Second hash
        threshold: Maximum Hamming distance for similarity
        
    Returns:
        True if the hashes are similar, False otherwise
    """
    return hamming_distance(hash1, hash2) <= threshold 