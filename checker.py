import sys

if len(sys.argv) <= 2:
    print('uso: $ {} <arquivo original> <arquivo ordenado>'.format(sys.argv[0]))
    sys.exit(1)

with open(sys.argv[1], 'r') as file:
    original = file.readlines()
    original = list(map(int, original))
    original.sort()

with open(sys.argv[2], 'r') as file:
    sorted = file.readlines()
    sorted = list(map(int, sorted))

if original == sorted:
    print('A ordenação está correta')
else:
    print('Há uma falha na ordenação')
    print(original)
    print(sorted)

