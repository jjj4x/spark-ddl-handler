from argparse import ArgumentParser
from pathlib import PosixPath

from spark_ddl_handler.cli.entrypoint import CLIEntrypoint

CLI = ArgumentParser()
CLI_COMMANDS = CLI.add_subparsers()
LOADER = CLI_COMMANDS.add_parser('load')
PARSER = CLI_COMMANDS.add_parser('parse')

SPARK_CONF_ARG = dict(
    nargs='*',
    default=(
        ('spark.driver', 'local[1]'),
        ('spark.app.name', 'SparkTableHandler'),
    ),
    help=(
        'SparkConf options passed like: '
        'spark.app.name=SparkTableHandler '
        "'spark.driver=local[1]' "
        '...'
    ),
    type=lambda opt: opt.split('='),
)

LOADER.add_argument(
    '--spark-conf',
    **SPARK_CONF_ARG,
)
LOADER.add_argument(
    '--schema-names',
    nargs='+',
    required=True,
    help='Schemas to load.',
)
LOADER.set_defaults(
    func=CLIEntrypoint('load'),
)

PARSER.add_argument(
    'input',
    nargs=1,
    help='Input file or "-" for stdin.',
)
PARSER.add_argument(
    '--output',
    default=PosixPath('./stubs/'),
    help='Output directory.',
    type=PosixPath,
)
PARSER.set_defaults(
    func=CLIEntrypoint('parse'),
)


def main():
    args = CLI.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
