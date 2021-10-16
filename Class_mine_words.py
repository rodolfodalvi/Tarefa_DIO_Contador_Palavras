from mrjob.job import MRJob
from mrjob.step import MRStep
import re

REGEX_ONLY_WORDS = "[\w']+"

class MRDataMining(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words, combiner=self.combiner_count_words),
            MRStep(reducer=self.reducer_max_word)
        ]

    def mapper_get_words(self, _, line):
        words = re.findall(REGEX_ONLY_WORDS, line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, values):
        yield word, sum(values)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))
        
    def reducer_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)

    

if __name__ == '__main__':
    MRDataMining.run()
