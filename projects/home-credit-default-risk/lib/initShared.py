import sys
import os

def initializeSharedLib():
    
    cwd = os.getcwd()
    projectDir = os.path.dirname(cwd)
    platformDir = os.path.dirname(projectDir)
    
    sharedDir = platformDir+'/shared'

    if sharedDir not in sys.path:
        sys.path.append(sharedDir)
    
    pass