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
                             keep=False,
                             inplace=True)
    # Resetting index to start from 0 and keep a correct sequence.
    csv_file.reset_index(inplace=True,
                         drop=True,
                         level=0)
    # Adding the new index (unique integer for each unique user) as column in dataframe
    csv_file[new_col] = csv_file.index
    return csv_file


def convert_full_frame(full_frame, new_sub_frame, new_col, mapped_column):
    # Creating a new column, with constant to be initialized (should be negative so no confusion with id's happen)
    # ID's start from 0 so no negative ID's
    full_frame[new_col] = -1

    # Setting both data frames same index to map the data together
    full_frame.set_index(mapped_column, inplace=True)

    # Updating the larger data frame using the index mapping
    # Replacing the -1 column with appropriate mapping.
    full_frame.update(new_sub_frame.set_index(mapped_column))

    full_frame[new_col] = full_frame[new_col].astype(int)

    # Re-shaping to starting structure.
    full_frame.reset_index(inplace=True, level=0, drop=False)

    return full_frame


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

    print(itemID_sorted)
    aggregate = convert_full_frame(full_CSV_file.copy(), userID_sorted.copy(), 'userIdAsInteger', 'userId')
    agg = convert_full_frame(full_CSV_file.copy(), itemID_sorted.copy(), 'itemIdAsInteger', 'itemId')
    print(agg)

    print(aggregate)
