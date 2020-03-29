"""Apache_beam test program."""
import apache_beam as beam
import re


def words_count(inputs_pattern, outputs_prefix):
    """Words_count function transform raw text."""
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Read lines'
            >> beam.io.ReadFromText(inputs_pattern)
            | 'Find words'
            >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line))
            | 'Pair words with start value'
            >> beam.Map(lambda word: (word, 1))
            | 'Group and sum words'
            >> beam.CombinePerKey(sum)
            | 'Formating results'
            >> beam.Map(lambda word_count: f'{word_count[0]}, {word_count[1]}')
            | 'Write results to file'
            >> beam.io.WriteToText(outputs_prefix, header='words,numbers',
                                   file_name_suffix='.csv')
        )


def main():
    """Realization of main_function."""
    inputs_pattern = 'input_data/react.txt'
    outputs_prefix = 'result'
    words_count(inputs_pattern, outputs_prefix)


if __name__ == '__main__':
    main()
