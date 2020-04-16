import os
import glob


class AbstractSampleOrdering:

    def __init__(self, path, pattern):
        self.files = glob.glob(os.path.join(path, pattern))

    def __len__(self):
        return len(self.files)

    def __add__(self, other):
        return self.files.extend(other.files)

    def get_ordering(self):
        raise NotImplementedError("Ordering must be defined.")


class KmerOrdering(AbstractSampleOrdering):

    def get_ordering(self):
        pass


class GCContentOrdering(AbstractSampleOrdering):

    def get_ordering(self):
        pass