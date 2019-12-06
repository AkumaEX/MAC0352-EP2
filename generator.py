import random


max = int(input('Number of Integers: '))
with open('generator_out.txt', 'w') as file:
    for i in range(0, max):
        number = random.randint(0, 4294967295)
        file.write(str(number) + '\n')
