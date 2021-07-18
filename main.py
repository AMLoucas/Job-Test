"""
Make sure all the packages are installed before trying to run the script.
Importing libraries and packages that will be used in the program.
"""
import csv

import pandas


# Method that reads a csv file and returns it to the main.
def read_csv_file(filename):
    df = pandas.read_csv(filename,
                         names=['userId', 'itemId', 'rating', 'timestamp'])
    return df


"""
This is the main method.
It is in charge of calling the correct process of the pipeline to accomplish the tasks in hand.
"""
# Conditional to start the main process.
if __name__ == '__main__':
    # Change the file path to suit your local directory.
    full_CSV_file = read_csv_file("/Users/louca5z/Downloads/xag.csv")
    print(full_CSV_file)
