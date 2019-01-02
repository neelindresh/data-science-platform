import sys

def initialize():
    sharedPath = '/home/jupuser/shared'

    if sharedPath not in sys.path:
        sys.path.append(sharedPath)
    
    pass