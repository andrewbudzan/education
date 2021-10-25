# author: andrii budzan

class Node:

    def __init__(self, data):
        self.data = data
        self.next_node = None


class LinkedList:

    def __init__(self):
        self.head = Node
        self.number_of_nodes = 0

    # this gives O(1) complexity - to insert new node in the beginning of linked list
    # we need to update just one reference
    def insert_start(self, data):
        self.number_of_nodes += 1
        new_node = Node(data)
        if not self.head:
            self.head = new_node
        else:
            new_node.next_node = self.head
            self.head = new_node

    # complexity will be O(N) because we need to start from the first element of list
    # and check all nodes if its references to None. this is the last node of linked list
    # and new node must be inserted right after current
    def insert_end(self, data):
        self.number_of_nodes += 1
        if not self.head:
            self.head = Node(data)
            return
        actual_node = self.head

        while actual_node.next_node:
            actual_node = actual_node.next_node
        actual_node.next_node = Node(data)

    def size_of_list(self):
        return self.number_of_nodes

    def traverse(self):
        actual_node = self.head
        while actual_node:
            print(actual_node.data)
            actual_node = actual_node.next_node


if __name__ == '__main__':
    linkedlist = LinkedList()
    linkedlist.insert_start(3)
    linkedlist.insert_end(323)
