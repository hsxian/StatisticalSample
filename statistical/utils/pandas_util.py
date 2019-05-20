import pandas as pd


def split_data_frame_list(df, target_column, output_type=float):
    ''' 
    Accepts a column with multiple types and splits list variables to several rows.

    df: dataframe to split
    target_column: the column containing the values to split
    output_type: type of all outputs
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''
    row_accumulator = []

    def split_list_to_rows(row):
        split_row = row[target_column]
        if isinstance(split_row, list):
            for s in split_row:
                new_row = row.to_dict()
                new_row[target_column] = s
                row_accumulator.append(new_row)
            if split_row == []:
                new_row = row.to_dict()
                new_row[target_column] = None
                row_accumulator.append(new_row)
        else:
            new_row = row.to_dict()
            new_row[target_column] = split_row
            row_accumulator.append(new_row)
    df.apply(split_list_to_rows, axis=1)
    new_df = pd.DataFrame(row_accumulator)
    return new_df

def test():
    df = pd.DataFrame({'name': ['a', 'b', 'c', 'd'], "items": [['a1', 'a2', 'a3'], [
        'b1', 'b2', 'b3'], ['c1', 'c2', 'c3'], []], 'leave me': range(4)})

    print(split_data_frame_list(df, 'items'))

