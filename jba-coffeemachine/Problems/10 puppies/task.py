class Puppy:
    n_puppies = 0  # number of created puppies

    def __new__(cls, *args, **kwargs):
        if Puppy.n_puppies < 10:
            Puppy.n_puppies += 1
            return object.__init__(cls)
