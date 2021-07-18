"""
Make sure all the packages are installed before trying to run the script.
Importing libraries and packages that will be used in the program.
"""
import pandas


# Method that reads a csv file and returns it to the main.
def read_csv_file(filename):
    df = pandas.read_csv(filename,
                         names=['userId', 'itemId', 'rating', 'timestamp'])
    return df


def convert_user_to_int(csv_file, sorted, drop_col, new_col):
    # Sorting by userID
    csv_file.sort_values(sorted, inplace=True)
    # Dropping columns not needed
    csv_file = csv_file.drop(drop_col, axis=1)
    # Removing duplicates of users.
    csv_file.drop_duplicates(subset=sorted,
                                  keep=False,
                                  inplace=True)
    # Resetting index to start from 0 and keep a correct sequence.
    csv_file.reset_index(inplace=True,
                         drop=True,
                         level=0)
    # Adding the new index (unique integer for each unique user) as column in datafram
    csv_file[new_col] = csv_file.index
    return csv_file


"""
This is the main method.
It is in charge of calling the correct process of the pipeline to accomplish the tasks in hand.
"""
# Conditional to start the main process.
if __name__ == '__main__':
    # Read the CSV file.
    # Change the file path to suit your local directory.
    full_CSV_file = read_csv_file("/Users/louca5z/Downloads/xag.csv")
    # print(full_CSV_file)

    # Converting the user ID's to integers.
    userID_sorted = convert_user_to_int(full_CSV_file,
                                        'userId',
                                        ['itemId', 'rating', 'timestamp'],
                                        'userIdAsInteger')

    # Converting the item ID's to integers.
    itemID_sorted = convert_user_to_int(full_CSV_file,
                                        'itemId',
                                        ['userId', 'rating', 'timestamp'],
                                        'itemIdAsInteger')
    print(itemID_sorted)
