import sys

def initializeSharedLib(sharedLibPath):    
    
    if sharedLibPath not in sys.path:
        sys.path.append(sharedLibPath)
    
    pass