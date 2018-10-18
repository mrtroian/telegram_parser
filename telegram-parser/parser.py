from sys import argv
from parsing import Parsing
from datetime import datetime
from config import keys

def main():
	try:
		if len(argv) < 3:
			print('''Options:\nparse_messages [group name]
				\rparse_participants [group name]\nget_statistics [group name | date from | date until]''')
			return 1
		parse = Parsing(argv[2])
		if argv[1] == "parse_messages":
			if len(argv) == 3:
				mod = 0
			elif len(argv) == 4:
				mod = int(argv[3])
			elif len(argv) == 5:
				mod = list()
				mod.append(datetime.strptime(argv[3], '%Y-%m-%d'))
				mod.append(datetime.strptime(argv[4], '%Y-%m-%d'))
			else:
				return 1
			parse.parse_messages(mod)
		elif argv[1] == "parse_participants":
			parse.parse_participants()
		elif argv[1] == "get_statistics":
			if len(argv) < 5:
				print('Invalid argument')
				return 1
			datum_range = parse.get_datum_range(argv[3], argv[4])
			if datum_range:
				print("There are %i messages during this period.\nDo you want to proceed?[y/n]" % datum_range[0])
			else:
				print("There are no messages during this period.")
				return
			if 'y' in input():
				parse.get_message_range(argv[3], argv[4], datum_range[1])
				print('Done')
		parse.finish()
		return 0
	except KeyboardInterrupt:
		print('Terminating...', end='\r')
		parse.finish()
		print('Terminated...')
		return 0
	except:
		print("Invalid argument")
		return 0

if __name__ == "__main__":
	main()