import argparse
import json

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/coders.py
class JSONCoder:
    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)


def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://bug-1506674/amiyaguchi/core-v10-nightly.json.gz",
        help="Input file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="gs://bug-1506674/processed/core-v10-nightly/",
        help="Output file to write results to.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(
        [
            "--runner=DataflowRunner",
            "--project=bug-1506674-main-pings ",
            "--staging_location=gs://bug-1506674/staging",
            "--temp_location=gs://bug-1506674/tmp",
            "--job_name=into-avro",
        ]
    )

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | ReadFromText(known_args.input, coder=JSONCoder())
            | WriteToAvro(known_args.output) # TODO: add schema
        )


if __name__ == "__main__":
    run()
