price_chicken = 23
price_goat = 678
price_pig = 1296
price_cow = 3848
price_sheep = 6769

user_money = int(input())


def purchase_calculation(money):
    if money >= price_sheep:
        can_buy = money // price_sheep
        print(f'{can_buy} sheep') # if can_buy > 1 else f'{can_buy} sheep')
    elif money >= price_cow:
        can_buy = money // price_cow
        print(f'{can_buy} cows' if can_buy > 1 else f'{can_buy} cow')
    elif money >= price_pig:
        can_buy = money // price_pig
        print(f'{can_buy} pigs' if can_buy > 1 else f'{can_buy} pig')
    elif money >= price_goat:
        can_buy = money // price_goat
        print(f'{can_buy} goats' if can_buy > 1 else f'{can_buy} goat')
    elif money >= price_chicken:
        can_buy = money // price_chicken
        print(f'{can_buy} chickens' if can_buy > 1 else f'{can_buy} chicken')
    else:
        print(None)


purchase_calculation(user_money)