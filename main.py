"""
Make sure all the packages are installed before trying to run the script.
Importing libraries and packages that will be used in the program.
"""
import pandas


# Method that reads a csv file and returns it to the main.
def read_csv_file(filename):
    df = pandas.read_csv(filename,
                         names=['userId', 'itemId', 'rating', 'timestamp'],
                         nrows=5000)
    return df


def convert_to_int(csv_file, sorting_value, drop_col, new_col):
    # Sorting by userID
    csv_file.sort_values(sorting_value, inplace=True)
    # Dropping columns not needed
    csv_file = csv_file.drop(drop_col, axis=1)
    # Removing duplicates of users.
    csv_file.drop_duplicates(subset=sorting_value,
                             keep='last',  # keep at least 1 record from the duplicates!!
                             inplace=True)
    # Resetting index to start from 0 and keep a correct sequence.
    csv_file.reset_index(inplace=True,
                         drop=True,
                         level=0)
    # Adding the new index (unique integer for each unique user) as column in dataframe
    csv_file[new_col] = csv_file.index
    return csv_file


def convert_full_frame(full_frame, new_sub_frame, mapped_column):
    # Merging two data frames and adding new value that corresponds to another dataframe.
    # Mapping the correct values.
    df = full_frame.merge(new_sub_frame, on=[mapped_column], how='left')

    # Removing old column that is not needed anymore.
    # String identifier column being removed.
    df = df.drop(mapped_column, axis=1)

    return df





"""
This is the main method.
It is in charge of calling the correct process of the pipeline to accomplish the tasks in hand.
"""
# Conditional to start the main process.
if __name__ == '__main__':
    # Read the CSV file.
    # Change the file path to suit your local directory.
    full_CSV_file = read_csv_file("/Users/louca5z/Downloads/xag.csv")

    # Converting the user ID's to integers.
    userID_sorted = convert_to_int(full_CSV_file.copy(),
                                   'userId',
                                   ['itemId', 'rating', 'timestamp'],
                                   'userIdAsInteger')

    # Converting the item ID's to integers.
    itemID_sorted = convert_to_int(full_CSV_file.copy(),
                                   'itemId',
                                   ['userId', 'rating', 'timestamp'],
                                   'itemIdAsInteger')

    aggregate = convert_full_frame(full_CSV_file.copy(), userID_sorted.copy(), 'userId')
    agg = convert_full_frame(aggregate.copy(), itemID_sorted.copy(), 'itemId')

    print(agg.info())


