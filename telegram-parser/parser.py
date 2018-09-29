from sys import argv
from parsing import parse_messages, parse_participants, telegram_init, get_datum_range, get_message_range
from config import keys

def main():
	try:
		if len(argv) < 2:
			print("Options:\nparse_messages\nparse_participants\nget_statistics")
		elif argv[1] == "parse_messages":
			if len(argv) < 4:
				print("Missing argument: [group, number of messages]")
				return 0
			parse_messages(telegram_init(), argv[2], int(argv[3]))
		elif argv[1] == "parse_participants":
			if len(argv) < 3:
				print("Missing argument: [group]")
				return 0
			parse_participants(telegram_init(), argv[2])
		elif argv[1] == "get_statistics":
			if len(argv) < 4:
				print("Missing arguments: [date from, date until]")
				return 0
			datum_range = get_datum_range(argv[2], argv[3])
			if datum_range:
				print("There are %i messages during this period.\nDo you want to proceed?[y/n]" % datum_range)
			else:
				print("There are no messages during this period.")
				return
			if 'y' in input():
				get_message_range(argv[2], argv[3])
			else:
				return 0
		else:
			print("Invalid argument")
			print("Options:\nparse_messages\nparse_participants\nget_statistics")
			return 0
		return 1
	except:
			print("Invalid argument")
			return 0

if __name__ == "__main__":
	main()