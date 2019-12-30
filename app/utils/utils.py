import os

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
    os.system('clear')
    for k, v in vals.items():
        percent = round(v * 100 / total, 2)
        p = round(v * scale / total)
        print('\'' + '#' * p + ' ' * (scale - p) + '\' ({}% - {})'.format(percent, k))
    print("Total tweets captured: {}".format(total))
