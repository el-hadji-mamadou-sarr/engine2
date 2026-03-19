

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

    def split_child(self, parent: Node, i: int):
        """split l'enfant i du parent"""
        child = parent.children[i]
        # new new node is on the right side
        new_node =Node(is_leaf=child.is_leaf)
        if child.is_leaf:
            i_median = t - 1
            new_node.keys=child.keys[i_median:]
            child.keys = child.keys[:i_median]
            new_node.next = child.next
            child.next = new_node
            
            # faire remonter la clé du milieu
            mid_key = new_node.keys[0]
        
        parent.keys.insert(i, mid_key)
        
    def insert(self, node: Node, key: int):
        if node.is_leaf:
            if not node.is_full():
                if not node.keys:
                    node.keys.append(key) 
                else:
                    i = 0
                    while i < len(node.keys):
                        if node.keys[i] > key:
                            break
                        i+=1
                    node.keys.insert(i, key)
            else:
                i_median = t - 1
                node.is_leaf = False
                new_node_left = Node(is_leaf=True)
                new_node_right = Node(is_leaf=True)
                
                new_node_left.keys=node.keys[:i_median]
                new_node_left.next = new_node_right
                
                new_node_right.keys = node.keys[i_median:]
                
                node.children.append(new_node_left)
                node.children.append(new_node_right)
                
                node.keys = [node.keys[i_median]]

        else:
            pass


tree = BplusTree()

tree.insert(1)
tree.insert(3)
tree.insert(5)
tree.insert(12)

tree.repr()