with open('columns.txt', 'r') as f:
    print([l.strip('\n') for l in f.readlines()])
