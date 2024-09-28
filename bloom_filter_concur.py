import hashlib
import pickle
import math
import mmap
import os
import threading
import queue
from functools import lru_cache

class FCFS_Semaphore:
    # The first come first serve semaphore must be used if we want the reader writer model to enforce order strictly, however, the increased overhead also needs to be acknowledged
    def __init__(self):
        self.semaphore = threading.Semaphore(1)
        self.service_queue = queue.Queue()

    def acquire(self):
        thread_id = threading.get_ident()
        self.service_queue.put(thread_id)

        while self.service_queue.queue[0] != thread_id:
            threading.Event().wait(0.1)

        self.semaphore.acquire()

    def release(self):
        self.semaphore.release()
        self.service_queue.get()

class BloomFilter:
    def __init__(self, num_elem, target_error_rate, filename="bloom_filter.bin") -> None:
        assert target_error_rate < 0.48, "Error rate must be smaller than around 0.5 for the filter to mean anything (else by the formula it would have negative length or 0 hash function)"
         # The metadata can be declared in the system memory since they don't scale, but they're still stored on disk to handle power cycle
        self.size = int(-num_elem * math.log(target_error_rate)/ (math.log(2) ** 2)) # The size of the bit array for the bloom filter
        self.hash_count = int(self.size/ num_elem * math.log(2)) # The number of hash functions to achieve desired error rate
        self.filename = filename
        max_cache_size = int(0.05 * self.size * self.hash_count)
        self.cache = lru_cache(maxsize=max_cache_size)(self.compute_hash)  # Cache up to a reasonable amount of hash results, for a balanced optimization

        # For concurrency
        self.readcount = 0 # Number of readers currently accessing resource
        self.resource = threading.Semaphore(1) # Can replace these with the FCFS semaphores
        self.rmutex = threading.Semaphore(1)
        self.serviceQueue = threading.Semaphore(1)

        if os.path.exists(self.filename):
            os.remove(self.filename)
        with open(filename, "wb") as f:
            f.write(b'\x00' * self.size)
        with open(self.filename, "r+b") as file:
            self.bit_array = mmap.mmap(file.fileno(), length=self.size) # Create the bit array directly in disk
        self.save_metadata()

    def compute_hash(self, elem: str, i: int) -> int:
        """Compute the hash of an element with an index i, and return the bit array index."""
        digest = hashlib.sha256((elem + str(i)).encode()).hexdigest() # Avoid using multiple hash functions by using the same one on altered elements
        return int(digest, 16) % self.size

    def add_elem(self, elem)-> None:
        """The api to add element"""
         # Entry for writer
        self.serviceQueue.acquire()
        self.resource.acquire()
        self.serviceQueue.release()

        self.load_metadata()
        for i in range(self.hash_count):
            index = self.compute_hash(str(elem), i)
            self.bit_array[index] = 1
        self.bit_array.flush() # Ensuring changes are not buffered and actually written to disk
        # print(f"added: {elem}")

        # Exit for writer
        self.resource.release()


    def check_member(self, elem)-> bool:
        """The api to check member"""
        # Entry for reader
        self.serviceQueue.acquire()
        self.rmutex.acquire()
        self.readcount += 1
        if self.readcount == 1:
            self.resource.acquire()
        self.serviceQueue.release()
        self.rmutex.release()

        self.load_metadata()
        res = True
        for i in range(self.hash_count):
            index = self.compute_hash(str(elem), i)
            if self.bit_array[index] != 1:
                res = False
                break
        # print(f"checked: {elem}")

        # Exit for reader
        self.rmutex.acquire()
        self.readcount -= 1
        if self.readcount == 0:
            self.resource.release()
        self.rmutex.release()

        return res
    
    def save_metadata(self) -> None:
        """Save parameters to handle power cycle"""
        metadata = {
            "size": self.size,
            "hash_count": self.hash_count
        }
        with open(self.filename + '.meta', 'wb') as f:
            pickle.dump(metadata, f)

    def load_metadata(self) -> None:
        """Load parameters to handle power cycle"""
        with open(self.filename + '.meta', 'rb') as f:
            metadata = pickle.load(f)
            self.size = metadata["size"]
            self.hash_count = metadata["hash_count"]

    def delete(self) -> None:
        """Remove filter from disk"""
        os.remove(self.filename)
        os.remove(self.filename + '.meta')
        
bf = BloomFilter(100, 0.1)
bf.add_elem(1)
bf.add_elem("abc")
bf.add_elem("def")
print(bf.check_member(1))
print(bf.check_member(2))
print(bf.check_member("abc"))
print(bf.check_member("def"))
print(bf.check_member("ab"))
print(bf.check_member("defg"))
