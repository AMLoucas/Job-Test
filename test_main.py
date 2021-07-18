'''
Test file that will apply Unit testing on methods of main.
Importing libraries needed to run the script.
'''
import pandas

import main

'''
Test making sure keeping higher rating than 0.01 working
'''


def test_keep_high_ratings():
    # Creating data
    data = {'rating': [0.0003, 1, 4]}
    df = pandas.DataFrame(data)

    # Running test
    assert len(main.keep_high_ratings(df)) == 2, "Should be 2 rows"


'''
Test making sure that removal of duplicates works appropriately.
'''


def test_remove_duplicates():
    # Creating data
    data = {'col': [1, 1, 2, 2, 4, 4, 4, 4]}
    df = pandas.DataFrame(data)

    # Running test
    assert len(main.remove_duplicates(df, 'col')) == 3, "Should be 3 rows"


'''
Test making sure the reading of file is correct and the type of data frame is correct together with the number of rows.
'''


def test_read_csv_file():
    # READING DATA
    df = main.read_csv_file("/Users/louca5z/Downloads/xag.csv")

    # CREATING DATA
    data = {'col': [1, 1, 2, 2, 4, 4, 4, 4]}
    df2 = pandas.DataFrame(data)

    assert type(df) == type(df2), "Should be a type class <class 'pandas.core.frame.DataFrame'>"
    assert len(df) != 0, "Should be larger than 0"


'''
Test checking that the rating sum calculation is correct.
'''


def test_find_rating_sum():
    # CREATING DATA
    data = {'userIdAsInteger': [1, 1],
            'itemIdAsInteger': [1, 1],
            'rating': [1, 1],
            'timestamp': [1, 1]}

    df = pandas.DataFrame(data)

    # Calling method.
    df2 = main.find_rating_sum(df)

    # Getting the sum value to assert
    value = df2['ratingSum'][0]

    # Asserting the value.
    assert value == 2, "Should be 2 the sum of ratings"


'''
Method checking that the damping factor is being applied correclty.
'''
def test_apply_decay_factor():
    # CREATING DATA
    data = {'userIdAsInteger': [1, 2],
            'itemIdAsInteger': [1, 2],
            'rating': [1, 1],
            'timestamp': [2, 1]}

    df = pandas.DataFrame(data)

    # Applying function.
    df2 = main.apply_decay_factor(df)

    # Pulling values to check if correct.
    value1 = df2['rating'][0]
    value2 = df2['rating'][1]

    # Value 1 should be 1*0.95
    assert value1 == 0.95, "It should be 0.95 rating"
    # Value 1 should be 1*0.95**2
    assert value2 == 0.9025, "It should be 0.9025 rating"




'''
Main method condition that will call the appropriate functions to test the files.
'''
if __name__ == "__main__":
    test_keep_high_ratings()
    print("Test-1 passed")
    test_remove_duplicates()
    print("Test-2 passed")
    test_read_csv_file()
    print("Test-3 passed")
    test_find_rating_sum()
    print("Test-4 passed")
    test_apply_decay_factor()
    print("Test-5 passed")
    print("All test have been passed.")
