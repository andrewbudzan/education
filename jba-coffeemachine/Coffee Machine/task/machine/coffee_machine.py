class CoffeeMachine:

    state = 'main'
    amount_money = 550
    amount_water = 400
    amount_milk = 540
    amount_disposable_cups = 9
    receipts = {
        1: {
            'name': 'espresso',
            'water': 250,
            'milk': 0,
            'beans': 16,
            'price': 4,
            'cup': 1
        },
        2: {
            'name': 'latte',
            'water': 350,
            'milk': 75,
            'beans': 20,
            'price': 7,
            'cup': 1
        },
        3: {
            'name': 'cappuccino',
            'water': 200,
            'milk': 100,
            'beans': 12,
            'price': 6,
            'cup': 1
        }
    }

    def __init__(self, money=550, water=400, milk=540,
                 beans=120, disposable_cups=9):
        self.amount_money = money
        self.amount_water = water
        self.amount_milk = milk
        self.amount_beans = beans
        self.amount_disposable_cups = disposable_cups

    def __str__(self):
        return f'state: {self.state}'

    def __repr__(self):
        return f'\nThe coffee machibe has:\n' + \
               f'{self.amount_water} of water\n' + \
               f'{self.amount_milk} of milk\n' + \
               f'{self.amount_beans} of beans\n' + \
               f'{self.amount_disposable_cups} of disposable cups\n' + \
               f'{self.amount_money} of money\n'

    # system logic
    def user_input(self):
        val = input('>>')
        if str(val).isdecimal():
            return int(val)
        else:
            return val

    def remaining(self):
        print(self.__repr__())

    # coffee buying process logic
    # must be:
    #   resource checking
    #   coffee_preparing
    def buy(self):
        self.state = 'coffee'
        print('What dp you want to buy? 1 - espresso, 2 - latte, '
              '3 - cappuccino, back - to main menu:')
        selected = self.user_input()
        if selected == 'back':
            print('Going back to main menu!')
        else:
            pass
            receipt = self.receipts[selected]
            if self.check_resources(receipt):
                self.prepare_coffee(receipt)
            else:
                pass

    def check_resources(self, receipt):
        self.state = 'coffee-checking-resources'
        keys = ['water', 'milk', 'beans', 'cup']
        available = [self.amount_water, self.amount_milk, self.amount_beans, self.amount_disposable_cups]
        needed = [receipt[k] for k in keys]
        i = 0
        for a, n in zip(available, needed):
            if a < n:
                print(f'Sorry, not enough {keys[i]}!')
                return False
            else:
                print('I have enough resources, making you a coffee!')
                return True

    def prepare_coffee(self, receipt):
        self.state = 'coffee-preparing-coffee'
        self.amount_water -= receipt['water']
        self.amount_milk -= receipt['milk']
        self.amount_beans -= receipt['beans']
        self.amount_disposable_cups -= receipt['cup']
        self.amount_money += receipt['price']

    # refilling logic
    def fill(self):
        self.state = 'resources'
        print('Please, add resources:')
        self.amount_water += int(input(f'{"water: ":>{8}}'))
        self.amount_milk += int(input(f'{"milk: ":>{8}}'))
        self.amount_beans += int(input(f'{"beans: ":>{8}}'))
        self.amount_disposable_cups += int(input(f'{"cups: ":>{8}}'))

    # money taking logic
    def take(self):
        self.state = 'money'
        print(f'I gave you {self.amount_money}$')
        self.amount_money = 0

    # exiting logic
    def exit(self):
        self.state = 'exit'

    # coffee machine navigation logic
    def start(self):
        self.state = 'main' if self.state != 'main' else self.state
        print('Please, select action: (buy, fill, take, exit, remaining):')
        option = self.user_input()
        if option == 'buy':
            self.buy()
        elif option == 'fill':
            self.fill()
        elif option == 'take':
            self.take()
        elif option == 'exit':
            self.exit()
        elif option == 'remaining':
            self.remaining()


if __name__=='__main__':

    coffee = CoffeeMachine()
    while coffee.state != 'exit':
        coffee.start()
