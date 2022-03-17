class Lightbulb:
    def __init__(self):
        self.state = "off"

    def change_state(self):
        message = 'Turning the light '
        if self.state == 'off':
            self.state = 'on'
            print(message + 'on')
        else:
            self.state = 'off'
            print(message + 'off')
