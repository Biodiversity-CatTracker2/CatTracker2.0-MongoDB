import datetime
import sys
import uuid
from pathlib import Path


def new_cat(cat_name):
	id_ = uuid.uuid5(uuid.NAMESPACE_DNS, cat_name)
	path = f'cats/{cat_name}-{str(id_).split("-")[0]}'
	Path(path).mkdir(exist_ok=True)
	Path(f'{path}/.{str(id_)}').touch()
	return path


def new_day(cat_name):
	path = f'cats/{cat_name}-{str(id_).split("-")[0]}'


if __name__ == '__main__':
	path = new_cat(sys.argv[2])
	if sys.argv[3] == '-d':
		if len(sys.argv) > 4:
			date = sys.argv[4]
		else:
			date = str(datetime.datetime.today().date().strftime('%m-%d-%Y'))
		Path(f'{path}/{date}').mkdir(exist_ok=True)
		for folder in ['RAW', 'KML', 'GPS', 'ACC']:
			Path(f'{path}/{date}/{folder}').mkdir(exist_ok=True)
		Path(f'{path}/{date}/ACC/DF4').mkdir(exist_ok=True)
