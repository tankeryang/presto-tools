import argparse


parser = argparse.ArgumentParser(prog="python3 prestoetl.py", description="This is a python etl script")
parser.add_argument(
    '--sql.names', action='store', dest='sql_names', nargs='*',
    help="set the sql file name for sql file, avalible to recieve multiple argment"
)
args = parser.parse_args()

print(args.sql_names)