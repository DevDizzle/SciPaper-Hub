
import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import aiplatform
from service.embed_vertex import embed_text

def parse_json(line):
    return json.loads(line)

def embed_and_format(record):
    try:
        abstract = record.get('abstract', '')
        if abstract:
            embedding = embed_text(abstract)
            return {
                'id': record['id'],
                'embedding': embedding,
                'title': record.get('title', ''),
                'authors': record.get('authors', ''),
                'doi': record.get('doi', '')
            }
    except Exception as e:
        logging.error(f"Error embedding record {record.get('id')}: {e}")
    return None

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input)
         | 'ParseJSON' >> beam.Map(parse_json)
         | 'EmbedAndFormat' >> beam.Map(embed_and_format)
         | 'FilterNone' >> beam.Filter(lambda x: x is not None)
         | 'WriteToGCS' >> beam.io.WriteToText(known_args.output, file_name_suffix='.json.gz')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    aiplatform.init(project='paperrec-ai', location='us-central1')
    run()
