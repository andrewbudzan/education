class PiggyBank:
    dollars = 1
    cents = 1

    def __init__(self, dollars, cents):
        self.dollars = dollars
        self.cents = cents

    def add_money(self, deposit_dollars, deposit_cents):
        self.dollars += deposit_dollars
        self.cents += deposit_cents
        while self.cents > 99:
            self.cents -= 100
            self.dollars += 1

    def show_money(self):
        print(f'Dollars: {self.dollars}. Cents: {self.cents}\n===\n')
        input('next')
