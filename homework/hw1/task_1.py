
from mrjob.job import MRJob
from mrjob.job import MRStep
import re


pattern = re.compile(r'\"([^\"]+)\"')


class Task1(MRJob):
    def mapper(self, _, line):
        matches = pattern.findall(line)
        character = matches[1]
        yield (character, 1)

    def combiner(self, character, counts):
        yield (character, sum(counts))
    
    def reducer(self, character, counts):
        yield None, (character, sum(counts))

    def find_top_20_characters(self, _, pairs):
        pairs = list(pairs)
        pairs.sort(key=lambda x: x[1], reverse=True)
        for character, count in pairs[:20]:
            yield (character, count)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.find_top_20_characters)
        ]
        
if __name__ == "__main__":
    Task1.run()
