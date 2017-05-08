if __name__ == '__main__':
    with open('sorted.blob', 'r') as f:
        prevLine = None
        for line in f:
            if prevLine is not None:
                if line.strip() < prevLine.strip():
                    print (line.strip(),prevLine.strip())
            prevLine = line