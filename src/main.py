import os

def main():
    try:
        DAYS_TO_KEEP = int(os.environ['DAYS_TO_KEEP'])
    except KeyError:
        DAYS_TO_KEEP = 7
        
    try:
        IF_COMPRESS = bool(os.environ['IS_COMPRESS'])
    except KeyError:
        IF_COMPRESS = True
    
    print(DAYS_TO_KEEP)
    print(IF_COMPRESS)

if __name__ == "__main__":
    main()