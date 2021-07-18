"""
Make sure all the packages are installed before trying to run the script.
Importing libraries and packages that will be used in the program.
"""
import pandas

'''
Method that reads the CSV file and applies headers to columns.
filename = variable with the full filepath of the file.
'''


def read_csv_file(filename):
    df = pandas.read_csv(filename,
                         names=['userId', 'itemId', 'rating', 'timestamp'],
                         nrows=5000)
    return df


'''
Method that drops all the extra dupllicate ID's (customer, item) and then converts the String ID's to
unique integer identifiers starting from 0 and having a correct sequence.
Works for User's and Item's
csv_file = the dataframe that was created from the CSV reader method.
sorting_value = the column we want to sort the dataframe.
drop_col = the columns we do not want to include in new dataframe.
new_col = the new column name we ant to add to the new dataframe.
'''
def convert_to_int(csv_file, sorting_value, drop_col, new_col):
    # Sorting by userID
    csv_file.sort_values(sorting_value, inplace=True)
    # Dropping columns not needed
    csv_file = csv_file.drop(drop_col, axis=1)

    # Calling function to remove duplicates
    csv_file = remove_duplicates(csv_file.copy(), sorting_value)

    # Resetting index to start from 0 and keep a correct sequence.
    csv_file.reset_index(inplace=True,
                         drop=True,
                         level=0)
    # Adding the new index (unique integer for each unique user) as column in dataframe
    csv_file[new_col] = csv_file.index
    return csv_file

'''
Function that removes duplicates values from column. However, it keeps one of the duplicates in the frame.
Does not delete all of the recods.
'''
def remove_duplicates(df, col_name):
    # Removing duplicates of column col_name.
    df.drop_duplicates(subset=col_name,
                             keep='last',  # keep at least 1 record from the duplicates!!
                             inplace=True)

    return df


'''
Method that replaces the String ID's with the new unique numeric id's
It maps the appropriate String ID's with numeric and replaces them.
full_frame = the frame we want append on and drop columns.
new_sub_frame = The data frame that holds the numeric ID's we want to append on the full frame
mapped_column = is the column we want to replace with the numeric ID's
'''


def convert_full_frame(full_frame, new_sub_frame, mapped_column):
    # Merging two data frames and adding new value that corresponds to another dataframe.
    # Mapping the correct values.
    df = full_frame.merge(new_sub_frame, on=[mapped_column], how='left')

    # Removing old column that is not needed anymore.
    # String identifier column being removed.
    df = df.drop(mapped_column, axis=1)

    return df


'''
This is a function that applies a multiplicative decay factor on the ratings depending on timestamp.
[NOTE] Honestly i did not understand what is being asked here precisely, so i researched how decay factors work
and assumed the following is correct. My understanding is based on this youtube video : https://www.youtube.com/watch?v=Q4WaHGgJMy0

which translated to => rating * (0.95) ^ [row(timestamp) / max(timestamp)] , This is what i applied.

df = Is the dataframe we will aplpy the computations and modifications on.
'''


def apply_decay_factor(df):
    # Constants that will be used in the process.
    decay_factor = 0.95
    maximum_timestamp = max(df['timestamp'])

    df['rating'] = df['rating'] * (decay_factor ** (df['timestamp'] / maximum_timestamp))

    return keep_high_ratings(df)


'''
Function that keeps only the ratings that are higher value than 0.01
data_frame = is the frame we will filter data out.
'''


def keep_high_ratings(data_frame):
    return data_frame[data_frame.rating > 0.01]


'''
Method that groups all the unique userId-itemId to tuples and computed the sum of all their ratings.
data_frame = Is a dataframe where we group the pairs and compute sum
'''
def find_rating_sum(data_frame):
    #
    data_frame = data_frame.drop('timestamp', axis=1)
    # Finding unique paris and finding the rating sum.
    new_df = data_frame.groupby(['userIdAsInteger', 'itemIdAsInteger'])['rating'].sum().reset_index()

    return new_df.rename(columns={'rating': 'ratingSum'})


'''
Method that will export the dataframe to a CSV file. The file will be exported on the code directory.
df = dataframe we are exporting
path = the filename we want to provide.
'''


def write_csv_result(df, path):
    df.to_csv(path, header=True)


"""
This is the main method.
It is in charge of calling the correct process of the pipeline to accomplish the tasks in hand.
"""
# Conditional to start the main process.
if __name__ == '__main__':
    # Print statement to notify user the script is running
    print("Script is being run ...")
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

    # Replacing the String ID's with numeric ID's.
    aggratings = convert_full_frame(full_CSV_file.copy(), userID_sorted.copy(), 'userId')
    aggratings = convert_full_frame(aggratings.copy(), itemID_sorted.copy(), 'itemId')

    # Applyin the decay factor
    aggratings = apply_decay_factor(aggratings.copy())

    # Computing sum of ratings.
    aggratings = find_rating_sum(aggratings.copy())

    # Exporting the data_frames to CSV files.
    # Writing the resultant dataframes to CSV files.
    write_csv_result(userID_sorted, 'lookupuser.csv')
    write_csv_result(itemID_sorted, 'lookup_product.csv')
    write_csv_result(userID_sorted, 'aggratings.csv')
