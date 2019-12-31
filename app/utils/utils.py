from os import system
import logging

def dash(vals, scale=150, take_only=20):
    take_only = min(len(vals), take_only)

    # This tends to overwhelm the screen when there is too much rows
    # We'll display only the largest N rows and aggregate the others into 'others' 
    # vals = {k: v for k, v in sorted(vals.items(), key=lambda x: x[1], reverse=True)}

    sorted_vals = sorted(vals.items(), key=lambda x: x[1], reverse=True)
    vals = {k: v for k, v in sorted_vals[:take_only]}
    vals['others'] = sum([v for k, v in sorted_vals[- take_only:]])

    total = sum(vals.values())

    # Clear the terminal
    system('clear')

    for k, v in vals.items():
        percent = round(v * 100 / total, 2)
        p = round(v * scale / total)
        print('\'' + '#' * p + ' ' * (scale - p) + '\' ({}% - {})'.format(percent, k))
    print("Total tweets captured: {}".format(total))

def init_logger():
    """
    Create and return a logger object
    """
    logger = logging.getLogger()

    # Create formatter with a specific format
    formatter = logging.Formatter("[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")

    # Create handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)

    # Assign the handler
    logger.addHandler(handler)
    return logger