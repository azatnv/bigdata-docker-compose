
from mrjob.job import MRJob
from mrjob.job import MRStep
import re

import nltk
from nltk.tokenize import word_tokenize


pattern = re.compile(r'\"([^\"]+)\"')


class Task3(MRJob):
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')
        self.stopwords = nltk.corpus.stopwords.words("english")
        
    def mapper(self, _, line):
        matches = pattern.findall(line)
        speech: str = matches[2].lower()
        speech: list = word_tokenize(speech)
        speech = [w for w in speech 
                    if w.isalpha() and w not in self.stopwords]
        
        if len(speech) <= 1:
            yield (None, 1)
        
        for i in range(len(speech) - 1):
            yield (f"{speech[i]} {speech[i + 1]}", 1)

    def combiner(self, bigram, counts):
        yield (bigram, sum(counts))
    
    def reducer(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def find_top_20_bigrams(self, _, pairs):
        pairs = list(pairs)
        pairs = [p for p in pairs if p[0] is not None]
        pairs.sort(key=lambda x: x[1], reverse=True)
        for bigram, count in pairs[:20]:
            yield (bigram, count)
    
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.find_top_20_bigrams)
        ]
        
if __name__ == "__main__":
    Task3.run()
