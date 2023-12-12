
from mrjob.job import MRJob
from mrjob.job import MRStep
import re


pattern = re.compile(r'\"([^\"]+)\"')


class Task2(MRJob):
    def mapper(self, _, line):
        matches = pattern.findall(line)
        character = matches[1]
        speech = matches[2]
        yield (character, speech)

    def combiner(self, character, speeches):
        speeches = list(speeches)
        longest_speech = speeches[0]
        for s in speeches:
            if s > longest_speech:
                longest_speech = s
        yield (character, longest_speech)
    
    def reducer(self, character, speeches):
        speeches = list(speeches)
        longest_speech = speeches[0]
        for s in speeches:
            if s > longest_speech:
                longest_speech = s
        yield None, (character, longest_speech)

    def rank_speeches(self, _, pairs):
        pairs = list(pairs)
        pairs.sort(key=lambda x: len(x[1]), reverse=True)
        for character, speech in pairs:
            yield (character, speech)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.rank_speeches)
        ]
        
if __name__ == "__main__":
    Task2.run()
