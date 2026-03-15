

t = 2

class Node:
    def __init__(self, is_leaf: bool):
        self.is_leaf = is_leaf
        
        self.keys: list[int] = []
        self.children: list[Node] = []
        
        self.next: Node = None

    def is_full(self):
        return len(self.keys) >= 2*t - 1
    
class BplusTree:
    root: Node = Node(is_leaf=True)
    
    def repr(self):
        children = self.root.children
        most_left_cild = children[0]
        next = most_left_cild.next
        print("Node[0] keys")
        print(most_left_cild.keys)
        i=1
        while next is not None:
            print(f"Node[{i}] keys")
            print(next.keys)
            next = next.next

        
    def insert(self, key: int):
        if self.root.is_leaf:
            if not self.root.is_full():
                if not self.root.keys:
                    self.root.keys.append(key) 
                else:
                    i = 0
                    while i < len(self.root.keys):
                        if self.root.keys[i] > key:
                            break
                        i+=1
                    self.root.keys.insert(i, key)
            else:
                i_median = t - 1
                self.root.is_leaf = False
                new_node_left = Node(is_leaf=True)
                new_node_right = Node(is_leaf=True)
                
                new_node_left.keys=self.root.keys[:i_median]
                new_node_left.next = new_node_right
                
                new_node_right.keys = self.root.keys[i_median:]
                
                self.root.children.append(new_node_left)
                self.root.children.append(new_node_right)
                
                self.root.keys = [self.root.keys[i_median]]

        else:
            pass
        

tree = BplusTree()

tree.insert(1)
tree.insert(3)
tree.insert(5)
tree.insert(12)

tree.repr()