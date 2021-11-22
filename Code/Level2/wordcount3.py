from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_find_max_word(self, _, word_count_pairs):
        try:
            yield max(word_count_pairs)
        except ValueError:
            pass

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]
if __name__ == '__main__':

    MRWordFreqCount.run()
    