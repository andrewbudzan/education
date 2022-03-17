# our class Ship
class Ship:
    def __init__(self, name, capacity,destination):
        self.name = name
        self.capacity = capacity
        self.cargo = 0
        self.desination = destination

    # the old sail method that you need to rewrite
    def sail(self):
        return f"The {self.name} has sailed for {self.desination}!"


black_pearl = Ship("Black Pearl", 800, input())
print(black_pearl.sail())