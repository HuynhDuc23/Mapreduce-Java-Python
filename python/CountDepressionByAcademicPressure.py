from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class CountDepressionByAcademicPressure(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_academic_pressure,
                   reducer=self.reducer_count_depression)
        ]

    def mapper_get_academic_pressure(self, _, line):
        # Ensure line is a string, if it is a byte string, decode it
        if isinstance(line, bytes):
            line = line.decode('utf-8')

        # Split the line into fields
        fields = line.split(',')
        # Skip the header line
        if fields[0] == "Gender":
            return
        # Extract the relevant fields
        academic_pressure = fields[2]
        depression = fields[10]
        # Emit key-value pair (academic_pressure, depression)
        yield academic_pressure, depression

    def reducer_count_depression(self, key, values):
        # Count the number of students with and without depression
        count_depression = 0
        count_no_depression = 0
        for value in values:
            if value.strip() == "Yes":
                count_depression += 1
            else:
                count_no_depression += 1
        # Emit the results
        yield key, {"Depression": count_depression, "No Depression": count_no_depression}

if __name__ == '__main__':
    # Redirecting stdin to read from CSV file directly
    sys.stdin = open("data.csv", "r")
    CountDepressionByAcademicPressure.run()
