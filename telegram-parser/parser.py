from sys import argv
from parsing import parse_messages, parse_participants, telegram_init
from config import keys

def main():
	try:
		if len(argv) < 2:
			print("Options:\nparse_messages\nparse_participants")
		elif argv[1] == "parse_messages":
			if len(argv) < 4:
				print("Missing argument")
				return 0
			parse_messages(telegram_init(), argv[2], int(argv[3]))
		elif argv[1] == "parse_participants":
			parse_participants(telegram_init(), argv[2])
		elif argv[1] == "get_statistics":
			update_azure_data(telegram_init(), argv[2])
		return 1
	except:
			print("Invalid argument")
			return 0

if __name__ == "__main__":
	main()